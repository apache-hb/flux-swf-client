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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.clients.sfn.TestConfig;
import com.danielgmyers.flux.clients.sfn.asl.AslGenerator;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
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

import software.amazon.awssdk.services.sfn.model.DescribeExecutionRequest;
import software.amazon.awssdk.services.sfn.model.DescribeExecutionResponse;
import software.amazon.awssdk.services.sfn.model.ExecutionStatus;
import software.amazon.awssdk.services.sfn.model.GetExecutionHistoryRequest;
import software.amazon.awssdk.services.sfn.model.GetExecutionHistoryResponse;
import software.amazon.awssdk.services.sfn.model.HistoryEvent;
import software.amazon.awssdk.services.sfn.model.HistoryEventType;

/**
 * Exercises Flux's error paths against real Step Functions. Verifies that when a step returns
 * {@link StepResult#failure()} and the graph has no fail-handler, the Choice state's default
 * route lands the execution in the shared terminal {@link AslGenerator#FAIL_STATE_NAME} state and
 * the SFN execution reports FAILED.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WorkflowFailureTest extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(WorkflowFailureTest.class);

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Arrays.asList(new FailingSingleStepWorkflow(), new FailingSecondStepWorkflow());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    @Test
    public void singleStepFailureLandsInFluxFailState() throws InterruptedException {
        String workflowId = "fail-" + UUID.randomUUID();

        WorkflowStatusChecker checker = executeWorkflow(FailingSingleStepWorkflow.class, workflowId,
                                                       Collections.emptyMap());
        WorkflowStatus status = waitForWorkflowCompletion(checker, Duration.ofMinutes(3));

        Assertions.assertEquals(WorkflowStatus.FAILED, status,
                                "Workflow should be FAILED when a step returns failure() and no fail-handler is defined");
        Assertions.assertTrue(AlwaysFailsStep.didExecute(workflowId),
                              "AlwaysFailsStep should have been invoked once before failing");
        Assertions.assertFalse(NeverReachedStep.didExecute(workflowId),
                               "NeverReachedStep is only reachable on success; it must not run");

        String executionArn = SfnArnFormatter.executionArn(TestConfig.getAwsRegion(),
                                                          TestConfig.getAwsAccountId(),
                                                          FailingSingleStepWorkflow.class, workflowId);
        assertExecutionEnteredFluxFailState(executionArn);
        assertExecutionFailedWithFluxTaskFailedError(executionArn);
    }

    @Test
    public void failureAfterPriorSuccessfulStepStillReachesFluxFail() throws InterruptedException {
        String workflowId = "fail-after-success-" + UUID.randomUUID();

        WorkflowStatusChecker checker = executeWorkflow(FailingSecondStepWorkflow.class, workflowId,
                                                       Collections.emptyMap());
        WorkflowStatus status = waitForWorkflowCompletion(checker, Duration.ofMinutes(3));

        Assertions.assertEquals(WorkflowStatus.FAILED, status);
        Assertions.assertTrue(FirstSuccessStep.didExecute(workflowId),
                              "First step should run and succeed before the failing step is reached");
        Assertions.assertTrue(SecondFailingStep.didExecute(workflowId),
                              "Second step should have been invoked once before failing");
        Assertions.assertFalse(NeverReachedStep.didExecute(workflowId),
                               "Workflow should not progress past the failing step");

        String executionArn = SfnArnFormatter.executionArn(TestConfig.getAwsRegion(),
                                                          TestConfig.getAwsAccountId(),
                                                          FailingSecondStepWorkflow.class, workflowId);
        assertExecutionEnteredFluxFailState(executionArn);
    }

    private void assertExecutionEnteredFluxFailState(String executionArn) {
        boolean enteredFailState = false;
        String token = null;
        do {
            GetExecutionHistoryResponse history = getSfnClient().getExecutionHistory(
                    GetExecutionHistoryRequest.builder()
                            .executionArn(executionArn)
                            .maxResults(1000)
                            .nextToken(token)
                            .build());
            for (HistoryEvent event : history.events()) {
                if (event.type() == HistoryEventType.FAIL_STATE_ENTERED
                        && event.stateEnteredEventDetails() != null
                        && AslGenerator.FAIL_STATE_NAME.equals(event.stateEnteredEventDetails().name())) {
                    enteredFailState = true;
                    break;
                }
            }
            token = history.nextToken();
        } while (!enteredFailState && token != null);

        Assertions.assertTrue(enteredFailState,
                              "Execution " + executionArn + " should have entered "
                              + AslGenerator.FAIL_STATE_NAME);
    }

    private void assertExecutionFailedWithFluxTaskFailedError(String executionArn) {
        DescribeExecutionResponse describe = getSfnClient().describeExecution(
                DescribeExecutionRequest.builder()
                        .executionArn(executionArn).build());
        Assertions.assertEquals(ExecutionStatus.FAILED, describe.status());
        // The shared Fail state is configured to report FluxTaskFailed as the error code. SFN exposes
        // the Fail state's Error verbatim on the execution's top-level error field.
        Assertions.assertEquals(AslGenerator.TASK_FAILED_ERROR, describe.error(),
                                "Execution top-level error should match the Fail state's Error code");
    }

    /** Single step that always returns {@link StepResult#failure()}. */
    public static final class FailingSingleStepWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public FailingSingleStepWorkflow() {
            AlwaysFailsStep failing = new AlwaysFailsStep();
            NeverReachedStep tail = new NeverReachedStep();
            // successTransition only — no fail-handler. When the step returns _fail, the generated
            // Choice state's default fires and routes to __FluxFail.
            this.graph = new WorkflowGraphBuilder(failing)
                    .successTransition(failing, tail)
                    .addStep(tail).alwaysClose(tail)
                    .build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    /** Two-step workflow: first step succeeds, second step fails with no fail-handler. */
    public static final class FailingSecondStepWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public FailingSecondStepWorkflow() {
            FirstSuccessStep first = new FirstSuccessStep();
            SecondFailingStep second = new SecondFailingStep();
            NeverReachedStep tail = new NeverReachedStep();
            this.graph = new WorkflowGraphBuilder(first)
                    .alwaysTransition(first, second)
                    .addStep(second).successTransition(second, tail)
                    .addStep(tail).alwaysClose(tail)
                    .build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static final class AlwaysFailsStep implements WorkflowStep {
        private static final Set<String> executedWorkflowIds = Collections.synchronizedSet(new HashSet<>());

        @StepApply
        public StepResult run(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId) {
            executedWorkflowIds.add(workflowId);
            return StepResult.failure("intentional integration-test failure");
        }

        static boolean didExecute(String workflowId) {
            return executedWorkflowIds.contains(workflowId);
        }
    }

    public static final class FirstSuccessStep implements WorkflowStep {
        private static final Set<String> executedWorkflowIds = Collections.synchronizedSet(new HashSet<>());

        @StepApply
        public StepResult run(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId) {
            executedWorkflowIds.add(workflowId);
            return StepResult.success();
        }

        static boolean didExecute(String workflowId) {
            return executedWorkflowIds.contains(workflowId);
        }
    }

    public static final class SecondFailingStep implements WorkflowStep {
        private static final Set<String> executedWorkflowIds = Collections.synchronizedSet(new HashSet<>());

        @StepApply
        public StepResult run(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId) {
            executedWorkflowIds.add(workflowId);
            return StepResult.failure("intentional integration-test failure on second step");
        }

        static boolean didExecute(String workflowId) {
            return executedWorkflowIds.contains(workflowId);
        }
    }

    /** Sentinel step that should never run in either failure scenario. */
    public static final class NeverReachedStep implements WorkflowStep {
        private static final Set<String> executedWorkflowIds = Collections.synchronizedSet(new HashSet<>());

        @StepApply
        public void run(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId) {
            executedWorkflowIds.add(workflowId);
        }

        static boolean didExecute(String workflowId) {
            return executedWorkflowIds.contains(workflowId);
        }
    }
}
