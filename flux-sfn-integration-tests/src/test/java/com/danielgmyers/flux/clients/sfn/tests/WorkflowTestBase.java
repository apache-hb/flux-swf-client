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

package com.danielgmyers.flux.clients.sfn.tests;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.RemoteWorkflowExecutor;
import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.clients.sfn.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.sfn.FluxCapacitorFactory;
import com.danielgmyers.flux.clients.sfn.TestConfig;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.WorkflowStatus;
import com.danielgmyers.flux.wf.graph.WorkflowGraphNode;
import com.danielgmyers.metrics.recorders.NoopMetricRecorderFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.SfnClientBuilder;
import software.amazon.awssdk.services.sfn.model.DeleteActivityRequest;
import software.amazon.awssdk.services.sfn.model.DeleteStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.DescribeStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.ExecutionListItem;
import software.amazon.awssdk.services.sfn.model.ExecutionStatus;
import software.amazon.awssdk.services.sfn.model.ListExecutionsRequest;
import software.amazon.awssdk.services.sfn.model.ListExecutionsResponse;
import software.amazon.awssdk.services.sfn.model.StateMachineDoesNotExistException;
import software.amazon.awssdk.services.sfn.model.StateMachineStatus;
import software.amazon.awssdk.services.sfn.model.StopExecutionRequest;
import software.amazon.awssdk.services.sfn.paginators.ListExecutionsIterable;

/**
 * Shared setup/teardown for SFN integration tests. Talks to a real Step Functions endpoint using the
 * caller's AWS credentials.
 *
 * <p>The base class is {@code @Disabled} so the tests don't run on every {@code mvn verify} — they
 * require the system properties documented in {@link TestConfig} and live AWS access. Subclasses
 * intentionally do not re-annotate; running them is opt-in by removing the {@code @Disabled} on the
 * subclass or by invoking surefire with the relevant property set.</p>
 */
@Disabled
public abstract class WorkflowTestBase {

    private SfnClient sfnClient;
    private FluxCapacitor capacitor;

    protected WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowClass, String workflowId,
                                                    Map<String, Object> input) {
        return capacitor.executeWorkflow(workflowClass, workflowId, input);
    }

    protected RemoteWorkflowExecutor getRemoteWorkflowExecutor() {
        return capacitor.getRemoteWorkflowExecutor("test");
    }

    abstract List<Workflow> getWorkflowsForTest();

    /**
     * Test classes should override this so that the logger has the test class name in it.
     */
    abstract Logger getLogger();

    protected int getWorkerPoolThreadCount() {
        return 1;
    }

    protected SfnClient getSfnClient() {
        return sfnClient;
    }

    @BeforeAll
    public void setUpFluxCapacitor() throws InterruptedException {
        sfnClient = createSfnClient();

        // Best-effort cleanup of any leftover state from a previous failed run.
        terminateOpenExecutions(getWorkflowsForTest());

        // SFN's DeleteStateMachine is asynchronous; if a previous run is still tearing the same
        // state machine down, CreateStateMachine throws StateMachineDeleting. Wait it out.
        waitForStateMachinesToBeDeletable(getWorkflowsForTest());

        capacitor = createFluxCapacitor(getWorkflowsForTest());
    }

    private void waitForStateMachinesToBeDeletable(List<Workflow> workflows) throws InterruptedException {
        Instant deadline = Instant.now().plus(Duration.ofMinutes(2));
        for (Workflow workflow : workflows) {
            String arn = SfnArnFormatter.workflowArn(TestConfig.getAwsRegion(), TestConfig.getAwsAccountId(),
                                                     workflow.getClass());
            while (true) {
                try {
                    var resp = sfnClient.describeStateMachine(
                            DescribeStateMachineRequest.builder()
                                    .stateMachineArn(arn).build());
                    if (resp.status() != StateMachineStatus.DELETING) {
                        break;
                    }
                } catch (StateMachineDoesNotExistException e) {
                    break;
                }
                if (Instant.now().isAfter(deadline)) {
                    throw new RuntimeException("Timed out waiting for state machine " + arn
                                               + " to finish deleting.");
                }
                getLogger().info("State machine {} is still being deleted; waiting...", arn);
                TimeUnit.SECONDS.sleep(5);
            }
        }
    }

    protected SfnClient createSfnClient() {
        SfnClientBuilder builder = SfnClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.of(TestConfig.getAwsRegion()));
        if (TestConfig.getSfnEndpoint() != null) {
            builder.endpointOverride(URI.create(TestConfig.getSfnEndpoint()));
        }
        return builder.build();
    }

    protected FluxCapacitor createFluxCapacitor(List<Workflow> workflows) {
        getLogger().info("Initializing Flux for region {}", TestConfig.getAwsRegion());
        FluxCapacitorConfig config = TestConfig.generateFluxConfig(getWorkerPoolThreadCount());
        config.setRemoteSfnClientConfigProvider(endpointId -> TestConfig.getRemoteSfnClientConfig());
        updateFluxCapacitorConfig(config);
        FluxCapacitor capacitor = FluxCapacitorFactory.create(new NoopMetricRecorderFactory(),
                                                              DefaultCredentialsProvider.create(), config);
        capacitor.initialize(workflows);
        getLogger().info("Finished initializing Flux.");
        return capacitor;
    }

    protected void updateFluxCapacitorConfig(FluxCapacitorConfig config) {
        // do nothing by default
    }

    @AfterAll
    public void cleanUpFluxCapacitor() throws InterruptedException {
        getLogger().info("Shutting down Flux...");
        if (capacitor != null) {
            capacitor.shutdown();
            capacitor.awaitTermination(30, TimeUnit.SECONDS);
        }
        terminateOpenExecutions(getWorkflowsForTest());
        // Intentionally leaving state machines/activities behind so the executions remain inspectable.
        // deleteRegisteredResources(getWorkflowsForTest());
        if (sfnClient != null) {
            sfnClient.close();
        }
    }

    /**
     * Terminates any currently-running executions of the workflows under test. Useful for cleaning up
     * between runs that may have left executions in flight.
     */
    protected void terminateOpenExecutions(List<Workflow> workflows) {
        for (Workflow workflow : workflows) {
            String stateMachineArn = SfnArnFormatter.workflowArn(TestConfig.getAwsRegion(),
                                                                  TestConfig.getAwsAccountId(),
                                                                  workflow.getClass());
            try {
                ListExecutionsIterable iterable = sfnClient.listExecutionsPaginator(
                        ListExecutionsRequest.builder()
                                .stateMachineArn(stateMachineArn)
                                .statusFilter(ExecutionStatus.RUNNING)
                                .build());
                for (ListExecutionsResponse page : iterable) {
                    for (ExecutionListItem item : page.executions()) {
                        getLogger().info("Stopping leftover execution {}", item.executionArn());
                        sfnClient.stopExecution(StopExecutionRequest.builder()
                                .executionArn(item.executionArn())
                                .cause("integration test cleanup")
                                .build());
                    }
                }
            } catch (StateMachineDoesNotExistException e) {
                // First run for this state machine — nothing to clean up.
            }
        }
    }

    /**
     * Deletes the state machines and activities created by Flux for the workflows under test, so the
     * next run starts from a clean slate. SFN deletes are asynchronous, but that's fine — the test
     * re-registers everything as part of {@code initialize()}.
     */
    protected void deleteRegisteredResources(List<Workflow> workflows) {
        for (Workflow workflow : workflows) {
            String stateMachineArn = SfnArnFormatter.workflowArn(TestConfig.getAwsRegion(),
                                                                  TestConfig.getAwsAccountId(),
                                                                  workflow.getClass());
            try {
                getLogger().info("Deleting state machine {}", stateMachineArn);
                sfnClient.deleteStateMachine(DeleteStateMachineRequest.builder()
                        .stateMachineArn(stateMachineArn).build());
            } catch (RuntimeException e) {
                getLogger().warn("Failed to delete state machine {}: {}", stateMachineArn, e.getMessage());
            }

            for (WorkflowGraphNode node : workflow.getGraph().getNodes().values()) {
                Class<? extends WorkflowStep> stepClass = node.getStep().getClass();
                String activityArn = SfnArnFormatter.activityArn(TestConfig.getAwsRegion(),
                                                                  TestConfig.getAwsAccountId(),
                                                                  workflow.getClass(),
                                                                  stepClass);
                try {
                    sfnClient.deleteActivity(DeleteActivityRequest.builder().activityArn(activityArn).build());
                } catch (RuntimeException e) {
                    getLogger().warn("Failed to delete activity {}: {}", activityArn, e.getMessage());
                }

                // Partitioned steps also have a dedicated generator activity that Flux registered.
                if (PartitionedWorkflowStep.class.isAssignableFrom(stepClass)) {
                    String generatorArn = SfnArnFormatter.partitionGeneratorActivityArn(
                            TestConfig.getAwsRegion(), TestConfig.getAwsAccountId(),
                            workflow.getClass(), stepClass);
                    try {
                        sfnClient.deleteActivity(DeleteActivityRequest.builder()
                                .activityArn(generatorArn).build());
                    } catch (RuntimeException e) {
                        getLogger().warn("Failed to delete generator activity {}: {}",
                                         generatorArn, e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Polls for the execution to reach a terminal state. Throws if it doesn't complete within {@code timeout}.
     */
    protected WorkflowStatus waitForWorkflowCompletion(WorkflowStatusChecker checker, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        WorkflowStatus status;
        do {
            status = checker.checkStatus();
            if (isTerminal(status)) {
                return status;
            }
            TimeUnit.SECONDS.sleep(2);
        } while (Instant.now().isBefore(deadline));
        throw new RuntimeException("Timed out waiting for workflow to complete; last status: " + status);
    }

    private static boolean isTerminal(WorkflowStatus status) {
        return status == WorkflowStatus.COMPLETED
                || status == WorkflowStatus.FAILED
                || status == WorkflowStatus.CANCELED
                || status == WorkflowStatus.TERMINATED
                || status == WorkflowStatus.TIMED_OUT;
    }
}
