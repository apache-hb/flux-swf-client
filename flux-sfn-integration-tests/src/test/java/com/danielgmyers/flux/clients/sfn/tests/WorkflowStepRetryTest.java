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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
 * Validates that a step which initially returns {@link StepResult#retry()} is re-invoked according
 * to the ASL Retry rule generated from the step's {@code @StepApply} annotation, and eventually
 * succeeds once the retry counter satisfies the step's success condition.
 *
 * <p>Uses a short initial retry delay (1s) to keep the test under a couple of minutes; flakiness
 * here would point to either generator misconfiguration or to SFN refusing the Retry rule.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WorkflowStepRetryTest extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(WorkflowStepRetryTest.class);

    private static final int RETRIES_BEFORE_SUCCESS = 2;

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new FlakyWorkflow());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    @Test
    public void retriesUntilStepSucceeds() throws InterruptedException {
        String workflowId = "retry-" + UUID.randomUUID();
        WorkflowStatusChecker checker = executeWorkflow(FlakyWorkflow.class, workflowId, Collections.emptyMap());

        WorkflowStatus status = waitForWorkflowCompletion(checker, Duration.ofMinutes(3));
        Assertions.assertEquals(WorkflowStatus.COMPLETED, status);
        Assertions.assertTrue(FlakyStep.attempts(workflowId) > RETRIES_BEFORE_SUCCESS,
                              "Step should have been invoked more than " + RETRIES_BEFORE_SUCCESS
                              + " times; was " + FlakyStep.attempts(workflowId));
    }

    public static final class FlakyWorkflow implements Workflow {
        private final WorkflowGraph graph;
        FlakyWorkflow() {
            WorkflowStep step = new FlakyStep();
            this.graph = new WorkflowGraphBuilder(step).alwaysClose(step).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static final class FlakyStep implements WorkflowStep {
        private static final ConcurrentHashMap<String, AtomicInteger> attemptsByWorkflowId = new ConcurrentHashMap<>();

        @StepApply(initialRetryDelaySeconds = 1, maxRetryDelaySeconds = 1,
                   retriesBeforeBackoff = 5, jitterPercent = 0)
        public StepResult run(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId) {
            int attempt = attemptsByWorkflowId
                    .computeIfAbsent(workflowId, id -> new AtomicInteger())
                    .incrementAndGet();
            if (attempt <= RETRIES_BEFORE_SUCCESS) {
                return StepResult.retry("not ready yet, attempt " + attempt);
            }
            return StepResult.success();
        }

        static int attempts(String workflowId) {
            AtomicInteger a = attemptsByWorkflowId.get(workflowId);
            return a == null ? 0 : a.get();
        }
    }
}
