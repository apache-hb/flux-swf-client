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

import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Periodic;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FluxCapacitorPeriodicTest {

    public static class StepOne implements WorkflowStep {
        @StepApply public void apply() {}
    }

    @Periodic(runInterval = 10, intervalUnits = TimeUnit.MINUTES)
    public static class PeriodicWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public PeriodicWorkflow() {
            StepOne one = new StepOne();
            this.graph = new WorkflowGraphBuilder(one).alwaysClose(one).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static class PlainWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public PlainWorkflow() {
            StepOne one = new StepOne();
            this.graph = new WorkflowGraphBuilder(one).alwaysClose(one).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    @Test
    public void isPeriodicWorkflowChecksAnnotation() {
        Assertions.assertTrue(FluxCapacitorImpl.isPeriodicWorkflow(new PeriodicWorkflow()));
        Assertions.assertFalse(FluxCapacitorImpl.isPeriodicWorkflow(new PlainWorkflow()));
    }

    @Test
    public void submitIntervalIsHalfRunIntervalClampedToFiveSeconds() {
        // 10 min run interval -> 5 min submit interval.
        Assertions.assertEquals(300L, FluxCapacitorImpl.computeSubmitIntervalSeconds(600L));
        // 1s run interval -> floor would be 0, clamps up to 5s.
        Assertions.assertEquals(5L, FluxCapacitorImpl.computeSubmitIntervalSeconds(1L));
        // 1-day run interval clamps down to 1 hour.
        Assertions.assertEquals(3600L, FluxCapacitorImpl.computeSubmitIntervalSeconds(86400L));
    }

    @Test
    public void periodicExecutionNameIncludesBucket() {
        String name = FluxCapacitorImpl.periodicExecutionName("MyWorkflow", 42L);
        Assertions.assertEquals("MyWorkflow-42", name);
        // The execution name must fit within SFN's 80-character limit.
        Assertions.assertTrue(name.length() <= 80);
    }

    @Test
    public void differentBucketIndicesGiveDifferentNames() {
        // The bucket index increases monotonically with wall-clock time, so it never collides.
        Assertions.assertNotEquals(
                FluxCapacitorImpl.periodicExecutionName("Foo", 1L),
                FluxCapacitorImpl.periodicExecutionName("Foo", 2L));
    }
}
