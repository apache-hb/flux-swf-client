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

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.WorkflowStatus;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies the worker's heartbeating against real Step Functions. A step that sleeps for over two
 * minutes is configured with a 30-second {@link WorkflowStep#activityTaskHeartbeatTimeout()}; SFN
 * will fail the task with {@code States.Heartbeat} if no heartbeat arrives within that window. The
 * worker sends heartbeats every {@code HEARTBEAT_INTERVAL} (10s by default) while the activity
 * thread is running, so a successful completion proves the heartbeat loop is functioning.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LongRunningStepHeartbeatTest extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(LongRunningStepHeartbeatTest.class);

    static final Duration STEP_DURATION = Duration.ofSeconds(135);
    static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(30);

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new LongRunningWorkflow());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    @Test
    public void longRunningStepCompletesViaHeartbeating() throws InterruptedException {
        String workflowId = "heartbeat-" + UUID.randomUUID();

        WorkflowStatusChecker checker = executeWorkflow(LongRunningWorkflow.class, workflowId,
                                                       Collections.emptyMap());

        // Give the workflow a generous window: the step itself sleeps ~135s, plus polling,
        // status-check cadence, and SFN-side propagation.
        WorkflowStatus status = waitForWorkflowCompletion(checker, Duration.ofMinutes(6));

        Assertions.assertEquals(WorkflowStatus.COMPLETED, status,
                                "Workflow should complete; failure here indicates SFN killed the activity for"
                                + " missing heartbeats (heartbeat timeout = " + HEARTBEAT_TIMEOUT.getSeconds() + "s,"
                                + " step duration = " + STEP_DURATION.getSeconds() + "s).");
        Assertions.assertTrue(LongRunningStep.didExecute(workflowId),
                              "Long-running step should have actually run for " + workflowId);
        Assertions.assertTrue(LongRunningStep.completedNormally(workflowId),
                              "Long-running step must have slept its full duration without being interrupted");
    }

    public static final class LongRunningWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public LongRunningWorkflow() {
            WorkflowStep step = new LongRunningStep();
            this.graph = new WorkflowGraphBuilder(step).alwaysClose(step).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static final class LongRunningStep implements WorkflowStep {
        private static final Set<String> startedWorkflowIds = Collections.synchronizedSet(new HashSet<>());
        private static final Set<String> completedWorkflowIds = Collections.synchronizedSet(new HashSet<>());

        /**
         * Override the heartbeat timeout to be much shorter than the step's runtime so the test
         * actually exercises the heartbeat loop. If heartbeating is broken, SFN will fail the
         * activity with {@code States.Heartbeat} ~30s in.
         */
        @Override
        public Duration activityTaskHeartbeatTimeout() {
            return HEARTBEAT_TIMEOUT;
        }

        @StepApply
        public void run(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId) throws InterruptedException {
            startedWorkflowIds.add(workflowId);
            try {
                TimeUnit.SECONDS.sleep(STEP_DURATION.getSeconds());
            } catch (InterruptedException e) {
                // If the worker interrupts us, it's because the heartbeat loop saw the task disappear
                // on the SFN side. Surface that explicitly so the test fails loudly.
                Thread.currentThread().interrupt();
                throw e;
            }
            completedWorkflowIds.add(workflowId);
        }

        static boolean didExecute(String workflowId) {
            return startedWorkflowIds.contains(workflowId);
        }

        static boolean completedNormally(String workflowId) {
            return completedWorkflowIds.contains(workflowId);
        }
    }
}
