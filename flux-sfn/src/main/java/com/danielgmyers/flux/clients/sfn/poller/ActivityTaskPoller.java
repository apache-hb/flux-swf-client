/*
 *   Copyright Flux Contributors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.danielgmyers.flux.clients.sfn.poller;

import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;

import com.danielgmyers.flux.clients.sfn.step.SfnStepInputAccessor;
import com.danielgmyers.flux.ex.FluxException;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.threads.BlockOnSubmissionThreadPoolExecutor;
import com.danielgmyers.flux.util.AwsRetryUtils;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.metrics.MetricRecorder;
import com.danielgmyers.metrics.MetricRecorderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskRequest;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskResponse;
import software.amazon.awssdk.services.sfn.model.SendTaskFailureRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskHeartbeatRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessRequest;
import software.amazon.awssdk.services.sfn.model.TaskDoesNotExistException;
import software.amazon.awssdk.services.sfn.model.TaskTimedOutException;

/**
 * Poller that requests and handles activity tasks for a single specific activity.
 */
public class ActivityTaskPoller implements Runnable {

    // package-private for test visibility
    static final String ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX = "Flux.ActivityTaskPoll";
    static final String SEND_TASK_SUCCESS_METRIC_PREFIX = "Flux.SendTaskSuccess";
    static final String SEND_TASK_FAILURE_METRIC_PREFIX = "Flux.SendTaskFailure";

    static final String WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME = "Flux.WorkerThreadAvailabilityWaitTime";
    static final String NO_ACTIVITY_TASK_TO_EXECUTE_METRIC_NAME = "Flux.NoActivityTaskToExecute";

    /**
     * Error code used by the activity worker to signal a retry to Step Functions. The generated ASL
     * has a Retry rule keyed on this code, which is how the SFN-backed Flux implements step retries.
     */
    public static final String RETRY_ERROR_CODE = "retry";

    // package-private for unit test access
    static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(10);
    static final String SUFFIX_INDICATING_TRUNCATION = " <truncated>";

    private final Logger log = LoggerFactory.getLogger(ActivityTaskPoller.class);

    private final MetricRecorderFactory metricsFactory;
    private final SfnClient sfn;
    private final String identity;
    private final String activityArnToPoll;
    private final Workflow workflow;
    private final WorkflowStep workflowStep;
    private final String metricsSuffix;
    private final boolean partitionGeneratorMode;

    private final BlockOnSubmissionThreadPoolExecutor workerThreadPool;

    /**
     * Constructs an activity poller for a regular (non-partition-generator) activity.
     */
    public ActivityTaskPoller(MetricRecorderFactory metricsFactory, SfnClient sfnClient, String identity,
                              String activityArnToPoll, Workflow workflow, WorkflowStep workflowStep,
                              BlockOnSubmissionThreadPoolExecutor workerThreadPool) {
        this(metricsFactory, sfnClient, identity, activityArnToPoll, workflow, workflowStep, workerThreadPool, false);
    }

    /**
     * Constructs an activity poller.
     *
     * @param metricsFactory - A factory that produces MetricRecorder objects for emitting workflow metrics.
     * @param sfnClient An already-configured Step Functions client to be used for polling.
     * @param identity The worker identity that the poller should report.
     * @param activityArnToPoll The ARN of the specific activity this poller should poll for.
     * @param workflow The workflow containing the step being polled for.
     * @param workflowStep The workflow step being polled for.
     * @param workerThreadPool The pool of threads available to hand activity tasks off to.
     * @param partitionGeneratorMode If true, invocations run the step's {@code @PartitionIdGenerator}
     *                               method (returning the list of partition inputs) instead of
     *                               {@code @StepApply}.
     */
    public ActivityTaskPoller(MetricRecorderFactory metricsFactory, SfnClient sfnClient, String identity,
                              String activityArnToPoll, Workflow workflow, WorkflowStep workflowStep,
                              BlockOnSubmissionThreadPoolExecutor workerThreadPool,
                              boolean partitionGeneratorMode) {
        this.metricsFactory = metricsFactory;
        this.sfn = sfnClient;

        if (identity == null || identity.isEmpty() || identity.length() > 80) {
            throw new IllegalArgumentException("Invalid identity for task poller, must be 1-80 characters: " + identity);
        }
        this.identity = identity;

        this.activityArnToPoll = activityArnToPoll;

        this.workflow = workflow;
        this.workflowStep = workflowStep;
        this.metricsSuffix = workflow.getClass().getSimpleName() + "." + workflowStep.getClass().getSimpleName()
                + (partitionGeneratorMode ? ".Generator" : "");
        this.partitionGeneratorMode = partitionGeneratorMode;

        this.workerThreadPool = workerThreadPool;
    }

    @Override
    public void run() {
        // Each Flux activity registers its own SFN activity ARN, and SFN's GetActivityTask is per-ARN.
        // That means we have one poller (or several) per activity ARN, which is many more pollers than the
        // SWF backend has. If we held a worker-pool permit during the long-poll, we'd quickly run out of
        // permits and starve some activities of polling coverage — work would land in SFN's queue and sit
        // there for up to the full 60-second long-poll timeout because no poller for that ARN was
        // currently mid-poll.
        //
        // To avoid that, the poll itself is run *without* a worker permit. Only after a task arrives do we
        // hand it off to the worker pool, blocking on the pool's submission semaphore at that point. This
        // keeps every activity ARN continuously covered by an in-flight long-poll regardless of how many
        // workers are currently busy.
        //
        // The poll-time metric recorder's lifetime now spans the poll + worker handoff. It's closed by the
        // dispatched runnable after it picks up off the worker pool's queue, so that the worker-thread
        // availability wait-time metric is captured against the same recorder.
        MetricRecorder metrics = metricsFactory.newMetricRecorder(this.getClass().getSimpleName());
        GetActivityTaskResponse task;
        try {
            task = pollForActivityTask(metrics);
        } catch (Throwable t) {
            metrics.close();
            log.debug("Got exception while polling for activity task", t);
            throw t;
        }
        if (task == null) {
            metrics.close();
            return;
        }

        // Block on worker-pool capacity here, not before the poll. The wait-time metric measures only this
        // window (task in hand, waiting for a worker thread); on a non-saturated host it should be near-zero.
        metrics.startDuration(WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME);
        try {
            workerThreadPool.execute(() -> {
                try (metrics) {
                    Duration waitTime = metrics.endDuration(WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME);
                    metrics.addDuration(formatWorkerThreadAvailabilityWaitTimeMetricName(metricsSuffix),
                                         waitTime);
                    executeWithHeartbeat(task);
                }
            });
        } catch (RejectedExecutionException e) {
            // Pool shutting down. The task will time out on the SFN side and be redelivered.
            metrics.close();
            log.warn("The activity thread pool rejected the task. This is usually because it is shutting down.", e);
        }
    }

    private GetActivityTaskResponse pollForActivityTask(MetricRecorder metrics) {
        try {
            GetActivityTaskRequest request = GetActivityTaskRequest.builder()
                    .activityArn(activityArnToPoll).workerName(identity).build();

            GetActivityTaskResponse task
                    = AwsRetryUtils.executeWithInlineBackoff(() -> sfn.getActivityTask(request),
                    20, Duration.ofSeconds(2), metrics, formatActivityTaskPollTimeMetricName(metricsSuffix));

            if (task == null || task.taskToken() == null || task.taskToken().isEmpty()) {
                metrics.addCount(NO_ACTIVITY_TASK_TO_EXECUTE_METRIC_NAME, 1.0);
                metrics.addCount(formatNoActivityTaskToExecuteMetricName(metricsSuffix), 1.0);
                return null;
            }

            log.debug("Polled for activity task and there was work to do.");
            return task;
        } catch (Throwable e) {
            log.warn("Got an unexpected exception when polling for an activity task.", e);
            throw e;
        }
    }

    private void executeWithHeartbeat(GetActivityTaskResponse task) {
        try (MetricRecorder metrics = metricsFactory.newMetricRecorder(this.getClass().getSimpleName()
                + ".executeWithHeartbeat")) {
            String activityExecutionTimeMetricName = formatActivityExecutionTimeMetricName(metricsSuffix);
            metrics.startDuration(activityExecutionTimeMetricName);

            // First we need to pull the workflow id out of the input, we use it for logging.
            SfnStepInputAccessor stepInput = new SfnStepInputAccessor(task.input());
            String workflowId = stepInput.getAttribute(String.class, StepAttributes.WORKFLOW_ID);

            ActivityExecutor executor = new ActivityExecutor(identity, stepInput, workflow, workflowStep,
                                                              metrics, metricsFactory, partitionGeneratorMode);

            Thread activityThread = new Thread(executor);
            activityThread.start();

            try {
                while (true) {
                    activityThread.join(HEARTBEAT_INTERVAL.toMillis());
                    if (!activityThread.isAlive()) {
                        log.debug("The activity thread has ended successfully.");
                        break;
                    }

                    try {
                        SendTaskHeartbeatRequest request
                                = SendTaskHeartbeatRequest.builder().taskToken(task.taskToken()).build();
                        log.debug("Sending heartbeat for workflow id {} at activity {}.", workflowId, activityArnToPoll);
                        sfn.sendTaskHeartbeat(request);
                        log.debug("Recorded heartbeat for workflow id {} at activity {}.", workflowId, activityArnToPoll);
                    } catch (TaskDoesNotExistException | TaskTimedOutException e) {
                        String message = "The heartbeat told us that the task no longer exists or timed out,"
                                + " so we'll abort this task: " + e.getMessage();
                        // If the activity task no longer exists or timed out,
                        // then there's no reason to continue it, since we can't report success or failure.
                        log.warn(message);
                        activityThread.interrupt();
                        activityThread.join(); // we interrupted it, so we'll wait for it to end

                        // We throw an exception here because we don't need to call SendTaskFailure in this case;
                        // the workflow definition itself should already handle this error case.
                        throw new FluxException(message);
                    } catch (Exception e) {
                        log.warn("Got an error while trying to record a heartbeat.", e);
                        // If we weren't able to heartbeat, we don't want to fail.
                        // It may have been transient and we'll make another attempt after HEARTBEAT_INTERVAL has passed.
                        // If it fails enough for the activity to time out, then the workflow definition's timeout logic kicks in.
                    }
                }
            } catch (InterruptedException e) {
                metrics.addCount(formatActivityThreadInterruptedMetricName(metricsSuffix), 1.0);
                // We don't really know what happened in this case, we'll use e.getCause() if it's not null, otherwise e.
                Throwable cause = (e.getCause() == null ? e : e.getCause());
                String msg = "Error executing activity task";
                log.warn(msg, cause);
                throw new RuntimeException(msg, cause);
            }

            metrics.endDuration(activityExecutionTimeMetricName);

            // at this point the executor should be done.
            StepResult result = executor.getResult();

            switch (result.getAction()) {
                case COMPLETE:
                    SendTaskSuccessRequest successRequest = SendTaskSuccessRequest.builder()
                            .taskToken(task.taskToken())
                            // Note that the output here is the JSON object containing the full set of input and output attributes
                            // for this step, so we can't truncate it. This does mean if we exceed the length limit of 262144
                            // bytes, then this API call will fail, but we wouldn't be able to run the next step anyway
                            // since the input would be malformed JSON if we truncated it.
                            // The only way out is for the user to deploy an updated step implementation that returns fewer output
                            // attributes.
                            .output(executor.getOutput())
                            .build();
                    AwsRetryUtils.executeWithInlineBackoff(() -> sfn.sendTaskSuccess(successRequest),
                            20, Duration.ofSeconds(2), metrics,
                            SEND_TASK_SUCCESS_METRIC_PREFIX);
                    break;
                case RETRY:
                    // Step Functions does support retry logic, but we have to configure it with an error code
                    // and associated retry settings. We'll return a special "retry" error code here
                    // and the graph generator will configure it to actually cause retries.
                    SendTaskFailureRequest failureRequest = SendTaskFailureRequest.builder()
                            .taskToken(task.taskToken())
                            .error(RETRY_ERROR_CODE)
                            // We truncate the retry cause if it's too long. This should be safe since this field is only meant for
                            // human consumption.
                            .cause(prepareRetryCause(executor.getRetryCause()))
                            .build();
                    AwsRetryUtils.executeWithInlineBackoff(() -> sfn.sendTaskFailure(failureRequest),
                            20, Duration.ofSeconds(2), metrics,
                            SEND_TASK_FAILURE_METRIC_PREFIX);
                    break;
                default:
                    throw new FluxException("Unknown result action: " + result.getAction());
            }
        } catch (Exception e) {
            log.warn("Got an exception executing the activity", e);
        }
    }

    // package-private for unit testing
    String prepareRetryCause(String cause) {
        // max length of the cause field in Step Functions is 32768 characters.
        if (cause != null && cause.length() > 32768) {
            log.warn("Retry cause will be truncated in the response to Step Functions: {}", cause);
            int len = 32768 - SUFFIX_INDICATING_TRUNCATION.length();
            return cause.substring(0, len) + SUFFIX_INDICATING_TRUNCATION;
        }
        return cause;
    }

    // package-private for test visibility
    static String formatNoActivityTaskToExecuteMetricName(String activityName) {
        return String.format("%s.%s", NO_ACTIVITY_TASK_TO_EXECUTE_METRIC_NAME, activityName);
    }

    // package-private for test visibility
    static String formatActivityTaskPollTimeMetricName(String activityName) {
        return String.format("%s.%s", ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX, activityName);
    }

    // package-private for test visibility
    static String formatWorkerThreadAvailabilityWaitTimeMetricName(String activityName) {
        return String.format("%s.%s", WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME, activityName);
    }

    // package-private for test visibility
    static String formatActivityThreadInterruptedMetricName(String activityName) {
        return String.format("Flux.ActivityThreadInterrupted.%s", activityName);
    }

    // package-private for test visibility
    static String formatActivityExecutionTimeMetricName(String activityName) {
        return String.format("Flux.ActivityExecutionTime.%s", activityName);
    }
}
