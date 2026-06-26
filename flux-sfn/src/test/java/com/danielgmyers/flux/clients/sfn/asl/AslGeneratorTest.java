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

package com.danielgmyers.flux.clients.sfn.asl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.danielgmyers.flux.clients.sfn.poller.ActivityTaskPoller;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.PartitionIdGenerator;
import com.danielgmyers.flux.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AslGeneratorTest {

    private static final String REGION = "us-east-1";
    private static final String ACCOUNT = "123456789012";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static class StepA implements WorkflowStep {
        @StepApply
        public void apply() {}
    }

    public static class StepB implements WorkflowStep {
        @StepApply
        public void apply() {}
    }

    public static class StepC implements WorkflowStep {
        @StepApply
        public StepResult apply() {
            return StepResult.success();
        }
    }

    public static class StepWithCustomBackoff implements WorkflowStep {
        @StepApply(initialRetryDelaySeconds = 30, maxRetryDelaySeconds = 300,
                   retriesBeforeBackoff = 2, jitterPercent = 0, exponentialBackoffBase = 2.0)
        public void apply() {}
    }

    /** Workflow with a single step that always closes — should not need a Choice state. */
    public static class SingleStepWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public SingleStepWorkflow() {
            StepA a = new StepA();
            this.graph = new WorkflowGraphBuilder(a).alwaysClose(a).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    /** Linear workflow A → B → close. */
    public static class LinearWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public LinearWorkflow() {
            StepA a = new StepA();
            StepB b = new StepB();
            this.graph = new WorkflowGraphBuilder(a)
                    .alwaysTransition(a, b)
                    .addStep(b).alwaysClose(b)
                    .build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    /** Branching workflow: A succeeds → B, A fails → C, both close. */
    public static class BranchingWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public BranchingWorkflow() {
            StepA a = new StepA();
            StepB b = new StepB();
            StepC c = new StepC();
            this.graph = new WorkflowGraphBuilder(a)
                    .successTransition(a, b)
                    .failTransition(a, c)
                    .addStep(b).alwaysClose(b)
                    .addStep(c).alwaysClose(c)
                    .build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    /** Workflow whose first step uses custom retry settings. */
    public static class CustomBackoffWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public CustomBackoffWorkflow() {
            StepWithCustomBackoff s = new StepWithCustomBackoff();
            this.graph = new WorkflowGraphBuilder(s).alwaysClose(s).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    @Test
    public void generatesValidJsonForSingleStepWorkflow() throws Exception {
        AslStateMachine sm = AslGenerator.generate(new SingleStepWorkflow(), REGION, ACCOUNT);
        Assertions.assertEquals("StepA", sm.getStartAt());

        // The state machine should have exactly the Task state, the Fail state (for uncaught errors),
        // and no Choice state since the only transition closes the workflow.
        Map<String, AslState> states = sm.getStates();
        Assertions.assertTrue(states.containsKey("StepA"));
        Assertions.assertFalse(states.containsKey("BranchStepA"));
        Assertions.assertTrue(states.containsKey(AslGenerator.FAIL_STATE_NAME));

        TaskState task = (TaskState) states.get("StepA");
        Assertions.assertEquals(Boolean.TRUE, task.getEnd());
        Assertions.assertNull(task.getNext());

        String json = sm.toJson();
        JsonNode tree = MAPPER.readTree(json);
        Assertions.assertEquals("StepA", tree.get("StartAt").asText());
        Assertions.assertTrue(tree.get("States").get("StepA").get("End").asBoolean());
    }

    @Test
    public void generatesLinearChainWithChoiceState() throws Exception {
        AslStateMachine sm = AslGenerator.generate(new LinearWorkflow(), REGION, ACCOUNT);

        Map<String, AslState> states = sm.getStates();
        Assertions.assertTrue(states.containsKey("StepA"));
        Assertions.assertTrue(states.containsKey("BranchStepA"));
        Assertions.assertTrue(states.containsKey("StepB"));
        // StepB always closes, so it should have End=true and no Choice.
        Assertions.assertFalse(states.containsKey("BranchStepB"));

        TaskState taskA = (TaskState) states.get("StepA");
        Assertions.assertEquals("BranchStepA", taskA.getNext());

        ChoiceState choiceA = (ChoiceState) states.get("BranchStepA");
        Assertions.assertEquals("StepB", choiceA.getDefaultNext());
        // The synthesized choice for an _always-only transition still includes one explicit Choice
        // since ASL requires Choices to be non-empty. The condition references the result-code.
        Assertions.assertEquals(1, choiceA.getChoices().size());
        Assertions.assertTrue(choiceA.getChoices().get(0).getCondition().contains(StepResult.ALWAYS_RESULT_CODE));
    }

    @Test
    public void branchingWorkflowEmitsChoiceForEachResultCode() throws Exception {
        AslStateMachine sm = AslGenerator.generate(new BranchingWorkflow(), REGION, ACCOUNT);

        ChoiceState choice = (ChoiceState) sm.getStates().get("BranchStepA");
        Assertions.assertNotNull(choice);
        Assertions.assertEquals(2, choice.getChoices().size());

        boolean sawSucceed = false;
        boolean sawFail = false;
        for (ChoiceState.Choice c : choice.getChoices()) {
            Assertions.assertTrue(c.getCondition().contains("$states.input." + StepAttributes.RESULT_CODE),
                                  "Condition should reference the result-code attribute: " + c.getCondition());
            if (c.getCondition().contains("'" + StepResult.SUCCEED_RESULT_CODE + "'")) {
                Assertions.assertEquals("StepB", c.getNext());
                sawSucceed = true;
            } else if (c.getCondition().contains("'" + StepResult.FAIL_RESULT_CODE + "'")) {
                Assertions.assertEquals("StepC", c.getNext());
                sawFail = true;
            }
        }
        Assertions.assertTrue(sawSucceed);
        Assertions.assertTrue(sawFail);
        // No "always" branch defined, so unexpected result codes route to the Fail state.
        Assertions.assertEquals(AslGenerator.FAIL_STATE_NAME, choice.getDefaultNext());
    }

    @Test
    public void taskStatesEmitActivityArnAndRetryRule() {
        AslStateMachine sm = AslGenerator.generate(new BranchingWorkflow(), REGION, ACCOUNT);

        TaskState task = (TaskState) sm.getStates().get("StepA");
        Assertions.assertEquals("arn:aws:states:us-east-1:123456789012:activity:BranchingWorkflow-StepA",
                                task.getResource());
        Assertions.assertEquals(1, task.getRetry().size());
        RetryRule r = task.getRetry().get(0);
        Assertions.assertEquals(List.of(ActivityTaskPoller.RETRY_ERROR_CODE), r.getErrorEquals());
        Assertions.assertEquals(10L, r.getIntervalSeconds().longValue()); // @StepApply default
        Assertions.assertEquals(600L, r.getMaxDelaySeconds().longValue()); // @StepApply default
        Assertions.assertEquals(RetryRule.JITTER_STRATEGY_FULL, r.getJitterStrategy());
        // default global backoff base used when @StepApply doesn't override it
        Assertions.assertEquals(1.25, r.getBackoffRate(), 0.0001);

        Assertions.assertEquals(1, task.getCatchRules().size());
        Assertions.assertEquals(AslGenerator.FAIL_STATE_NAME, task.getCatchRules().get(0).getNext());
    }

    @Test
    public void customStepApplyOverridesRetryConfig() {
        AslStateMachine sm = AslGenerator.generate(new CustomBackoffWorkflow(), REGION, ACCOUNT);
        TaskState task = (TaskState) sm.getStates().get("StepWithCustomBackoff");
        RetryRule r = task.getRetry().get(0);
        Assertions.assertEquals(30L, r.getIntervalSeconds().longValue());
        Assertions.assertEquals(300L, r.getMaxDelaySeconds().longValue());
        Assertions.assertEquals(2.0, r.getBackoffRate(), 0.0001);
        Assertions.assertNull(r.getJitterStrategy(), "jitter should be off when jitterPercent=0");
    }

    @Test
    public void jsonShapeIsValidAndStable() throws Exception {
        String json = AslGenerator.generateJson(new LinearWorkflow(), REGION, ACCOUNT);

        JsonNode root = MAPPER.readTree(json);
        Assertions.assertTrue(root.has("StartAt"));
        Assertions.assertTrue(root.has("States"));
        Assertions.assertEquals("StepA", root.get("StartAt").asText());

        JsonNode states = root.get("States");
        Assertions.assertEquals("Task", states.get("StepA").get("Type").asText());
        Assertions.assertEquals("Choice", states.get("BranchStepA").get("Type").asText());
        Assertions.assertEquals("Task", states.get("StepB").get("Type").asText());
        // StepB terminates via End=true on the Task, so no shared __FluxSucceed state is needed.
        Assertions.assertNull(states.get(AslGenerator.SUCCEED_STATE_NAME));
        Assertions.assertEquals("Fail", states.get(AslGenerator.FAIL_STATE_NAME).get("Type").asText());

        // Pretty form should also parse to the same tree.
        String pretty = AslGenerator.generatePrettyJson(new LinearWorkflow(), REGION, ACCOUNT);
        Assertions.assertEquals(root, MAPPER.readTree(pretty));
    }

    @Test
    public void rejectsNullArguments() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> AslGenerator.generate(null, REGION, ACCOUNT));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> AslGenerator.generate(new LinearWorkflow(), null, ACCOUNT));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> AslGenerator.generate(new LinearWorkflow(), REGION, ""));
    }

    public static class PartitionedStep implements PartitionedWorkflowStep {
        @PartitionIdGenerator
        public PartitionIdGeneratorResult genIds() {
            return PartitionIdGeneratorResult.create(Set.of("a", "b"));
        }

        @StepApply
        public void apply(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {}
    }

    public static class PartitionedWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public PartitionedWorkflow() {
            PartitionedStep p = new PartitionedStep();
            this.graph = new WorkflowGraphBuilder(p).alwaysClose(p).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static class PartitionedThenStepB implements Workflow {
        private final WorkflowGraph graph;
        public PartitionedThenStepB() {
            PartitionedStep p = new PartitionedStep();
            StepB b = new StepB();
            this.graph = new WorkflowGraphBuilder(p)
                    .alwaysTransition(p, b)
                    .addStep(b).alwaysClose(b)
                    .build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    @Test
    public void partitionedStepEmitsGeneratorTaskAndMapState() {
        AslStateMachine sm = AslGenerator.generate(new PartitionedWorkflow(), REGION, ACCOUNT);

        Assertions.assertEquals("PartitionedStepPartitionGenerator", sm.getStartAt());

        // Generator Task → invokes the *_gen activity, output lifts partition_ids to a known field.
        TaskState generator = (TaskState) sm.getStates().get("PartitionedStepPartitionGenerator");
        Assertions.assertNotNull(generator);
        Assertions.assertEquals(
                "arn:aws:states:us-east-1:123456789012:activity:PartitionedWorkflow-PartitionedStep_gen",
                generator.getResource());
        Assertions.assertEquals("PartitionedStep", generator.getNext());
        Assertions.assertNotNull(generator.getOutput(),
                                 "Generator should have an Output expression merging partition_ids into the input");

        // Map state iterates $states.input.partition_ids and runs the partition activity per item.
        MapState mapState = (MapState) sm.getStates().get("PartitionedStep");
        Assertions.assertNotNull(mapState);
        Assertions.assertEquals("{% $states.input.partition_ids %}", mapState.getItems());
        Assertions.assertEquals(Boolean.TRUE, mapState.getEnd(),
                                "Map should terminate when the partitioned step's only transition closes the workflow");

        // The inline iteration state invokes the partition activity (without the _gen suffix).
        MapState.ItemProcessor processor = mapState.getItemProcessor();
        Assertions.assertEquals(AslGenerator.PARTITION_ITERATION_STATE, processor.getStartAt());
        TaskState iteration = (TaskState) processor.getStates().get(AslGenerator.PARTITION_ITERATION_STATE);
        Assertions.assertEquals(
                "arn:aws:states:us-east-1:123456789012:activity:PartitionedWorkflow-PartitionedStep",
                iteration.getResource());
        Assertions.assertEquals(Boolean.TRUE, iteration.getEnd());
    }

    @Test
    public void partitionedStepWithFollowingStepTransitionsDirectlyToNextStep() {
        AslStateMachine sm = AslGenerator.generate(new PartitionedThenStepB(), REGION, ACCOUNT);

        // Partitioned steps only succeed (failures route to FAIL_STATE_NAME via the Map's catch), so the
        // Map state's Next points directly at the next step rather than going through a Choice state.
        MapState mapState = (MapState) sm.getStates().get("PartitionedStep");
        Assertions.assertEquals("StepB", mapState.getNext());
        Assertions.assertNull(mapState.getEnd());
        Assertions.assertFalse(sm.getStates().containsKey("BranchPartitionedStep"));
    }

}
