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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
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
 * Validates a multi-step workflow with branching transitions: the first step routes to one of two
 * follow-up steps depending on its result code. Verifies attribute passing between steps.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MultiStepWorkflowTest extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(MultiStepWorkflowTest.class);

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new BranchingWorkflow());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    @Override
    protected int getWorkerPoolThreadCount() {
        // This workflow has three activities; if there's only one worker thread, the SFN long-poll
        // on one activity will starve the others and the test will time out.
        return 3;
    }

    @Test
    public void successPath() throws InterruptedException {
        String workflowId = "branching-success-" + UUID.randomUUID();
        WorkflowStatusChecker checker = executeWorkflow(BranchingWorkflow.class, workflowId,
                                                         Map.of("shouldSucceed", Boolean.TRUE));
        WorkflowStatus status = waitForWorkflowCompletion(checker, Duration.ofMinutes(5));

        Assertions.assertEquals(WorkflowStatus.COMPLETED, status);
        Assertions.assertEquals("ran-success-step", SuccessStep.lastInvocation(workflowId));
        Assertions.assertNull(FailureStep.lastInvocation(workflowId));
    }

    @Test
    public void failurePath() throws InterruptedException {
        String workflowId = "branching-failure-" + UUID.randomUUID();
        WorkflowStatusChecker checker = executeWorkflow(BranchingWorkflow.class, workflowId,
                                                         Map.of("shouldSucceed", Boolean.FALSE));
        WorkflowStatus status = waitForWorkflowCompletion(checker, Duration.ofMinutes(5));

        Assertions.assertEquals(WorkflowStatus.COMPLETED, status);
        Assertions.assertNull(SuccessStep.lastInvocation(workflowId));
        Assertions.assertEquals("ran-failure-step", FailureStep.lastInvocation(workflowId));
    }

    public static final class BranchingWorkflow implements Workflow {
        private final WorkflowGraph graph;
        BranchingWorkflow() {
            WorkflowStep deciding = new DecideStep();
            WorkflowStep success = new SuccessStep();
            WorkflowStep failure = new FailureStep();
            this.graph = new WorkflowGraphBuilder(deciding,
                                                  Map.of("shouldSucceed", Boolean.class))
                    .successTransition(deciding, success)
                    .failTransition(deciding, failure)
                    .addStep(success).alwaysClose(success)
                    .addStep(failure).alwaysClose(failure)
                    .build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static final class DecideStep implements WorkflowStep {
        @StepApply
        public StepResult decide(@Attribute("shouldSucceed") Boolean shouldSucceed) {
            return Boolean.TRUE.equals(shouldSucceed) ? StepResult.success() : StepResult.failure();
        }
    }

    public static final class SuccessStep implements WorkflowStep {
        private static final Map<String, String> invocations = new ConcurrentHashMap<>();

        @StepApply
        public void run(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId) {
            invocations.put(workflowId, "ran-success-step");
        }

        static String lastInvocation(String workflowId) {
            return invocations.get(workflowId);
        }
    }

    public static final class FailureStep implements WorkflowStep {
        private static final Map<String, String> invocations = new ConcurrentHashMap<>();

        @StepApply
        public void run(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId) {
            invocations.put(workflowId, "ran-failure-step");
        }

        static String lastInvocation(String workflowId) {
            return invocations.get(workflowId);
        }
    }
}
