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
 * Validates the simplest possible end-to-end workflow against real Step Functions:
 * register a single-step workflow, start an execution, and confirm the activity worker
 * executed it and the workflow reached COMPLETED.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BasicWorkflowTest extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(BasicWorkflowTest.class);

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new BasicHelloWorld());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    @Test
    public void testBasicWorkflow() throws InterruptedException {
        String workflowId = "basic-" + UUID.randomUUID();

        WorkflowStatusChecker checker = executeWorkflow(BasicHelloWorld.class, workflowId, Collections.emptyMap());
        WorkflowStatus status = waitForWorkflowCompletion(checker, Duration.ofMinutes(3));

        Assertions.assertEquals(WorkflowStatus.COMPLETED, status);
        Assertions.assertTrue(BasicStepOne.didExecute(workflowId),
                              "Activity worker should have run BasicStepOne for " + workflowId);
    }

    @Test
    public void executeWorkflowIsIdempotentOnSameWorkflowId() throws InterruptedException {
        String workflowId = "idempotent-" + UUID.randomUUID();

        WorkflowStatusChecker first = executeWorkflow(BasicHelloWorld.class, workflowId, Collections.emptyMap());
        // Second call with same id must not throw; SFN's name-uniqueness gives idempotency.
        WorkflowStatusChecker second = executeWorkflow(BasicHelloWorld.class, workflowId, Collections.emptyMap());
        Assertions.assertNotNull(first);
        Assertions.assertNotNull(second);

        WorkflowStatus status = waitForWorkflowCompletion(first, Duration.ofMinutes(3));
        Assertions.assertEquals(WorkflowStatus.COMPLETED, status);
        Assertions.assertTrue(BasicStepOne.didExecute(workflowId));
    }

    /** Single-step workflow that records each id it was invoked with. */
    public static final class BasicHelloWorld implements Workflow {
        private final WorkflowGraph graph;
        public BasicHelloWorld() {
            WorkflowStep stepOne = new BasicStepOne();
            this.graph = new WorkflowGraphBuilder(stepOne, Collections.emptyMap()).alwaysClose(stepOne).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static final class BasicStepOne implements WorkflowStep {
        private static final Set<String> executedWorkflowIds = Collections.synchronizedSet(new HashSet<>());

        @StepApply
        public void doThing(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId) {
            executedWorkflowIds.add(workflowId);
        }

        static boolean didExecute(String workflowId) {
            return executedWorkflowIds.contains(workflowId);
        }
    }
}
