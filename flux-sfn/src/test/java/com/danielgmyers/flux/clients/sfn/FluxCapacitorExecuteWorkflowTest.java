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

package com.danielgmyers.flux.clients.sfn;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.clients.sfn.step.SfnStepInputAccessor;
import com.danielgmyers.flux.ex.WorkflowExecutionException;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.testutil.ManualClock;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.danielgmyers.metrics.recorders.NoopMetricRecorderFactory;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.ExecutionAlreadyExistsException;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse;

public class FluxCapacitorExecuteWorkflowTest {

    private static final String REGION = "us-east-1";
    private static final String ACCOUNT = "123456789012";

    public static class StepOne implements WorkflowStep {
        @StepApply public void apply() {}
    }

    public static class TestWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public TestWorkflow() {
            StepOne one = new StepOne();
            this.graph = new WorkflowGraphBuilder(one).alwaysClose(one).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static class NotRegisteredWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public NotRegisteredWorkflow() {
            StepOne one = new StepOne();
            this.graph = new WorkflowGraphBuilder(one).alwaysClose(one).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    private IMocksControl mockery;
    private SfnClient sfn;
    private FluxCapacitorImpl fc;
    private FluxCapacitorConfig config;
    private ManualClock clock;

    @BeforeEach
    public void setup() {
        mockery = EasyMock.createControl();
        sfn = mockery.createMock(SfnClient.class);

        config = new FluxCapacitorConfig();
        config.setAwsRegion(REGION);
        config.setAwsAccountId(ACCOUNT);
        config.setStateMachineRoleArn("arn:aws:iam::123456789012:role/sfn-role");

        clock = new ManualClock(Instant.parse("2026-01-01T00:00:00Z"));
        fc = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), sfn, config, clock);
        fc.populateMaps(List.of(new TestWorkflow()));
    }

    @Test
    public void throwsIfNotInitialized() {
        FluxCapacitorImpl emptyFc = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), sfn, config, clock);
        Assertions.assertThrows(WorkflowExecutionException.class,
                () -> emptyFc.executeWorkflow(TestWorkflow.class, "wf-1", Map.of()));
    }

    @Test
    public void throwsForUnregisteredWorkflow() {
        Assertions.assertThrows(WorkflowExecutionException.class,
                () -> fc.executeWorkflow(NotRegisteredWorkflow.class, "wf-1", Map.of()));
    }

    @Test
    public void startsExecutionWithExpectedRequestShape() throws Exception {
        AtomicReference<StartExecutionRequest> captured = new AtomicReference<>();
        String executionArn = "arn:aws:states:" + REGION + ":" + ACCOUNT + ":execution:TestWorkflow:wf-id";
        EasyMock.expect(sfn.startExecution(EasyMock.<StartExecutionRequest>anyObject()))
                .andAnswer(() -> {
                    captured.set((StartExecutionRequest) EasyMock.getCurrentArguments()[0]);
                    return StartExecutionResponse.builder().executionArn(executionArn).build();
                });

        mockery.replay();
        WorkflowStatusChecker checker = fc.executeWorkflow(TestWorkflow.class, "wf-id", Map.of("count", 7L));
        mockery.verify();

        Assertions.assertNotNull(checker);
        StartExecutionRequest req = captured.get();
        Assertions.assertEquals("wf-id", req.name());
        Assertions.assertEquals("arn:aws:states:" + REGION + ":" + ACCOUNT + ":stateMachine:TestWorkflow",
                                req.stateMachineArn());

        // The input JSON carries only the user's payload. Built-in attributes (workflow id, execution
        // id, start time) are injected later by the generated ASL from $states.context.Execution.*.
        SfnStepInputAccessor accessor = new SfnStepInputAccessor(req.input());
        Assertions.assertEquals(7L, accessor.getAttribute(Long.class, "count"));
        Assertions.assertNull(accessor.getAttribute(String.class, StepAttributes.WORKFLOW_ID),
                              "Workflow id should be sourced from SFN context, not injected at StartExecution");
        Assertions.assertNull(accessor.getAttribute(String.class, StepAttributes.WORKFLOW_EXECUTION_ID));
        Assertions.assertNull(accessor.getAttribute(Long.class, StepAttributes.WORKFLOW_START_TIME));
    }

    @Test
    public void treatsAlreadyExistsAsIdempotentSuccess() {
        EasyMock.expect(sfn.startExecution(EasyMock.<StartExecutionRequest>anyObject()))
                .andThrow(ExecutionAlreadyExistsException.builder().message("already there").build());

        mockery.replay();
        WorkflowStatusChecker checker = fc.executeWorkflow(TestWorkflow.class, "wf-id", Map.of());
        mockery.verify();
        Assertions.assertNotNull(checker);
    }

    @Test
    public void wrapsOtherFailuresInWorkflowExecutionException() {
        EasyMock.expect(sfn.startExecution(EasyMock.<StartExecutionRequest>anyObject()))
                .andThrow(new IllegalStateException("boom")).anyTimes();

        mockery.replay();
        Assertions.assertThrows(WorkflowExecutionException.class,
                () -> fc.executeWorkflow(TestWorkflow.class, "wf-id", Map.of()));
    }
}
