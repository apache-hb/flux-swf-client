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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.danielgmyers.flux.clients.sfn.poller.ActivityTaskPoller;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.internal.WorkflowStepUtil;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphNode;

/**
 * Generates an Amazon States Language (ASL) state machine definition from a Flux {@link Workflow}.
 *
 * <p>This class is the public, externally-consumable entry point for producing ASL JSON from a
 * Flux workflow definition. It is independent of any AWS SDK calls: callers can use it to inspect
 * or persist the ASL definition without needing AWS credentials.</p>
 *
 * <p>The generator emits state machines in <strong>JSONata</strong> mode. Built-in attributes that
 * historically Flux injected into the StartExecution input ({@code _h_workflow_id},
 * {@code _execution_id}, {@code _workflow_start_time}) are instead sourced from
 * {@code $states.context.Execution.*} inside each Task state's {@code Arguments} block, so Step
 * Functions itself is the source of truth for those fields.</p>
 *
 * <p>State naming convention:</p>
 * <ul>
 *   <li>{@code <StepName>} — Task state that invokes the activity for that step.</li>
 *   <li>{@code Branch<StepName>} — Choice state that branches on the step's result code per the graph's transitions.</li>
 *   <li>{@code __FluxSucceed} — single shared terminal Succeed state.</li>
 *   <li>{@code __FluxFail} — single shared terminal Fail state for uncaught task errors.</li>
 *   <li>{@code <StepName>PartitionGenerator} (partitioned steps) — Task state for the partition-id generator activity.</li>
 *   <li>{@code <StepName>} (partitioned steps) — Map state that iterates the partition activity per partition ID.</li>
 * </ul>
 *
 * <p>Each Task state has a single Retry rule keyed on the special error code
 * {@link ActivityTaskPoller#RETRY_ERROR_CODE "retry"} that the activity worker emits when a
 * step's {@code @StepApply} method returns {@link StepResult#retry()} or throws.</p>
 */
public final class AslGenerator {

    /** Name of the shared terminal Succeed state. */
    public static final String SUCCEED_STATE_NAME = "__FluxSucceed";

    /** Name of the shared terminal Fail state used when an activity errors out non-retryably. */
    public static final String FAIL_STATE_NAME = "__FluxFail";

    /** Error code emitted as the Fail state's Error field when a task fails outside the retry rule. */
    public static final String TASK_FAILED_ERROR = "FluxTaskFailed";

    /** State name of the inline Task that runs each partition iteration inside a Map state. */
    public static final String PARTITION_ITERATION_STATE = "Iteration";

    private AslGenerator() {}

    /**
     * Generates the ASL state machine for the given workflow.
     *
     * @param workflow     the Flux workflow whose graph should be converted
     * @param awsRegion    the AWS region in which the activities are registered (used to build activity ARNs)
     * @param awsAccountId the AWS account in which the activities are registered (used to build activity ARNs)
     */
    public static AslStateMachine generate(Workflow workflow, String awsRegion, String awsAccountId) {
        if (workflow == null) {
            throw new IllegalArgumentException("workflow must not be null");
        }
        if (awsRegion == null || awsRegion.isEmpty()) {
            throw new IllegalArgumentException("awsRegion must not be empty");
        }
        if (awsAccountId == null || awsAccountId.isEmpty()) {
            throw new IllegalArgumentException("awsAccountId must not be empty");
        }

        WorkflowGraph graph = workflow.getGraph();
        if (graph == null) {
            throw new IllegalArgumentException("workflow.getGraph() must not return null");
        }

        // The Fail state is always emitted because every Task/Map state gets a States.ALL Catch rule
        // that routes to it; the Succeed state is only emitted when a transition actually targets it.
        Map<String, AslState> states = new LinkedHashMap<>();
        TransitionResolver resolver = new TransitionResolver();
        for (Map.Entry<Class<? extends WorkflowStep>, WorkflowGraphNode> entry : graph.getNodes().entrySet()) {
            addStepStates(workflow, entry.getKey(), entry.getValue(), awsRegion, awsAccountId, states, resolver);
        }
        if (resolver.succeedStateNeeded) {
            addState(states, SUCCEED_STATE_NAME, new SucceedState("Workflow completed successfully."));
        }
        addState(states, FAIL_STATE_NAME,
                 new FailState(TASK_FAILED_ERROR, "An activity failed without being retried.", null));

        String startAt = entryStateName(graph.getFirstStep().getClass());
        long workflowTimeoutSeconds = workflow.maxStartToCloseDuration().getSeconds();
        Long timeoutSeconds = workflowTimeoutSeconds > 0 ? workflowTimeoutSeconds : null;
        return new AslStateMachine("Generated by Flux for workflow " + workflow.getClass().getSimpleName(),
                                   startAt, "JSONata", timeoutSeconds, states);
    }

    private static void addState(Map<String, AslState> states, String name, AslState state) {
        if (states.putIfAbsent(name, state) != null) {
            throw new IllegalArgumentException("Duplicate state name: " + name);
        }
    }

    private static void addStepStates(Workflow workflow,
                                      Class<? extends WorkflowStep> stepClass,
                                      WorkflowGraphNode node,
                                      String awsRegion,
                                      String awsAccountId,
                                      Map<String, AslState> states,
                                      TransitionResolver resolver) {
        WorkflowStep step = node.getStep();
        Map<String, WorkflowGraphNode> transitions = node.getNextStepsByResultCode();

        TransitionTargets targets = resolver.resolve(transitions);

        if (PartitionedWorkflowStep.class.isAssignableFrom(stepClass)) {
            addPartitionedStepStates(workflow, stepClass, step, targets, awsRegion, awsAccountId, states);
        } else {
            addRegularStepStates(workflow, stepClass, step, transitions, targets, awsRegion, awsAccountId, states);
        }
    }

    /** Resolved next-state names for the step's outgoing transitions. */
    private static final class TransitionTargets {
        private final Map<String, String> byResultCode = new LinkedHashMap<>();
        private String defaultNext;
        private boolean allTerminal;

        Map<String, String> getByResultCode() {
            return byResultCode;
        }

        String getDefaultNext() {
            return defaultNext;
        }

        boolean isAllTerminal() {
            return allTerminal;
        }
    }

    /**
     * Converts each step's raw graph transitions into ASL-target state names, tracking whether the
     * shared terminal Succeed state was referenced and so needs to be emitted.
     */
    private static final class TransitionResolver {
        private boolean succeedStateNeeded;

        TransitionTargets resolve(Map<String, WorkflowGraphNode> transitions) {
            TransitionTargets targets = new TransitionTargets();
            targets.allTerminal = !transitions.isEmpty() && transitions.values().stream().allMatch(n -> n == null);

            if (targets.allTerminal) {
                return targets;
            }

            String alwaysTarget = null;
            if (transitions.containsKey(StepResult.ALWAYS_RESULT_CODE)) {
                alwaysTarget = nextStateName(transitions.get(StepResult.ALWAYS_RESULT_CODE));
            }

            for (Map.Entry<String, WorkflowGraphNode> t : transitions.entrySet()) {
                if (StepResult.ALWAYS_RESULT_CODE.equals(t.getKey())) {
                    continue;
                }
                targets.byResultCode.put(t.getKey(), nextStateName(t.getValue()));
            }

            targets.defaultNext = alwaysTarget != null ? alwaysTarget : FAIL_STATE_NAME;
            return targets;
        }

        private String nextStateName(WorkflowGraphNode target) {
            if (target == null) {
                succeedStateNeeded = true;
                return SUCCEED_STATE_NAME;
            }
            return entryStateName(target.getStep().getClass());
        }
    }

    /**
     * The JSONata expression that produces the activity's input: state input merged with the three
     * context-sourced built-in attributes. Step Functions evaluates this {@code {% %}} block at runtime,
     * so the workflow id / execution id / workflow start time are always sourced from SFN itself
     * rather than from whatever the caller injected into the StartExecution input.
     */
    static final String STEP_ARGUMENTS_EXPR =
            "{% $merge([$states.input, {"
            + "'" + StepAttributes.WORKFLOW_ID + "': $states.context.StateMachine.Name, "
            + "'" + StepAttributes.WORKFLOW_EXECUTION_ID + "': $states.context.Execution.Name, "
            + "'" + StepAttributes.WORKFLOW_START_TIME + "': $toMillis($states.context.Execution.StartTime)"
            + "}]) %}";

    private static final List<CatchRule> CATCH_ALL_TO_FAIL_STATE
            = List.of(new CatchRule(List.of("States.ALL"), FAIL_STATE_NAME));

    private static void addRegularStepStates(Workflow workflow,
                                             Class<? extends WorkflowStep> stepClass,
                                             WorkflowStep step,
                                             Map<String, WorkflowGraphNode> transitions,
                                             TransitionTargets targets,
                                             String awsRegion,
                                             String awsAccountId,
                                             Map<String, AslState> states) {
        String stepStateName = stepStateName(stepClass);
        String activityArn = SfnArnFormatter.activityArn(awsRegion, awsAccountId, workflow.getClass(), stepClass);
        Long heartbeatSeconds = heartbeatSecondsOrNull(step);

        boolean end = targets.isAllTerminal();
        String next = end ? null : choiceStateName(stepClass);

        addState(states, stepStateName, new TaskState(null, activityArn, heartbeatSeconds,
                                                      STEP_ARGUMENTS_EXPR, null,
                                                      List.of(buildRetryRule(step)), CATCH_ALL_TO_FAIL_STATE,
                                                      next, end));

        if (!end) {
            addState(states, next, buildChoiceState(stepClass, transitions, targets));
        }
    }

    /**
     * Emit ASL for a partitioned step:
     *   XPartitionGenerator (Task: invokes generator activity, returns a flat object
     *     {@code {partition_ids: [...], user-attr-1: value-1, ...}}) merged with state input.
     *   → X (Map: iterates partition_ids, builds per-iteration input via ItemSelector)
     *   → next step (or End=true if all transitions close). No Choice state is needed because partitioned
     *     steps only succeed/fail; failures go to FAIL_STATE_NAME via the Map's catch rule.
     */
    private static void addPartitionedStepStates(Workflow workflow,
                                                 Class<? extends WorkflowStep> stepClass,
                                                 WorkflowStep step,
                                                 TransitionTargets targets,
                                                 String awsRegion,
                                                 String awsAccountId,
                                                 Map<String, AslState> states) {
        String generatorStateName = generatorStateName(stepClass);
        String mapStateName = stepStateName(stepClass);
        String generatorActivityArn = SfnArnFormatter.partitionGeneratorActivityArn(awsRegion, awsAccountId,
                                                                                     workflow.getClass(), stepClass);
        String partitionActivityArn = SfnArnFormatter.activityArn(awsRegion, awsAccountId,
                                                                   workflow.getClass(), stepClass);
        Long heartbeatSeconds = heartbeatSecondsOrNull(step);

        // 1) Generator Task. Same Arguments shape as a regular task. The generator returns a flat
        //    object {partition_ids: [...], user-attr: value, ...}; the Output expression merges that
        //    directly back into the state input so any extra attributes become regular state attributes.
        addState(states, generatorStateName, new TaskState(
                "Generates partition ids for " + stepClass.getSimpleName(),
                generatorActivityArn, null, STEP_ARGUMENTS_EXPR,
                "{% $merge([$states.input, $states.result]) %}",
                List.of(buildRetryRule(step)), CATCH_ALL_TO_FAIL_STATE,
                mapStateName, false));

        // 2) Map state. Items = $states.input.partition_ids. Each iteration builds its input from the
        //    Map state's input PLUS the partition-specific attributes via ItemSelector. Inside the
        //    ItemProcessor, $states.input is the value the ItemSelector built for this iteration —
        //    already merged and including the partition attributes.
        TaskState iteration = new TaskState(null, partitionActivityArn, heartbeatSeconds,
                                            "{% $states.input %}", null,
                                            List.of(buildRetryRule(step)), null, null, true);
        MapState.ItemProcessor processor = new MapState.ItemProcessor(
                PARTITION_ITERATION_STATE, Map.of(PARTITION_ITERATION_STATE, iteration));

        // Per-iteration input: the Map state's input (preserving the user's attributes) merged with the
        // SFN-context-sourced built-ins and the partition-specific attributes.
        String itemSelectorExpression =
                "{% $merge([$states.input, {"
                + "'" + StepAttributes.WORKFLOW_ID + "': $states.context.StateMachine.Name, "
                + "'" + StepAttributes.WORKFLOW_EXECUTION_ID + "': $states.context.Execution.Name, "
                + "'" + StepAttributes.WORKFLOW_START_TIME
                + "': $toMillis($states.context.Execution.StartTime), "
                + "'" + StepAttributes.PARTITION_ID + "': $states.context.Map.Item.Value, "
                + "'" + StepAttributes.PARTITION_COUNT + "': $count($states.input.partition_ids)"
                + "}]) %}";

        // Partitioned-step outputs are intentionally discarded by Flux. The Map state's input (which is
        // the generator Task's output) is preserved as the Map's output so downstream states still see
        // the merged attribute map.
        boolean end = targets.isAllTerminal();
        // Partitioned steps only support _succeed/_fail/_always. Map failures are routed to FAIL_STATE_NAME
        // by the catch rule, so successful Map completion always goes to the same place — no Choice state
        // is needed, just point Next directly at the succeed/_always target.
        String next = end ? null
                : targets.getByResultCode().getOrDefault(StepResult.SUCCEED_RESULT_CODE, targets.getDefaultNext());

        addState(states, mapStateName, new MapState(
                "Iterates partitions for " + stepClass.getSimpleName(),
                "{% $states.input.partition_ids %}", itemSelectorExpression, processor,
                "{% $states.input %}", null, CATCH_ALL_TO_FAIL_STATE, next, end));
    }

    private static Long heartbeatSecondsOrNull(WorkflowStep step) {
        long heartbeatSeconds = step.activityTaskHeartbeatTimeout().getSeconds();
        return heartbeatSeconds > 0 ? heartbeatSeconds : null;
    }

    private static ChoiceState buildChoiceState(Class<? extends WorkflowStep> stepClass,
                                                Map<String, WorkflowGraphNode> transitions,
                                                TransitionTargets targets) {
        List<ChoiceState.Choice> choices = new ArrayList<>();
        for (Map.Entry<String, String> t : targets.getByResultCode().entrySet()) {
            String condition = "{% $states.input." + StepAttributes.RESULT_CODE
                               + " = '" + t.getKey() + "' %}";
            choices.add(new ChoiceState.Choice(condition, t.getValue()));
        }

        // If the only transition is _always, the byResultCode map is empty. ASL requires Choices to be
        // non-empty, so add a no-op condition matching the always code. Default fires for everything
        // else (and points at the same target since "_always" already collapses paths).
        if (choices.isEmpty() && transitions.containsKey(StepResult.ALWAYS_RESULT_CODE)) {
            String condition = "{% $states.input." + StepAttributes.RESULT_CODE
                               + " = '" + StepResult.ALWAYS_RESULT_CODE + "' %}";
            choices.add(new ChoiceState.Choice(condition, targets.getDefaultNext()));
        }

        return new ChoiceState("Branches on " + StepAttributes.RESULT_CODE + " for step " + stepClass.getSimpleName(),
                               choices, targets.getDefaultNext());
    }

    /**
     * Convenience: generate the compact JSON form of the workflow's ASL definition.
     */
    public static String generateJson(Workflow workflow, String awsRegion, String awsAccountId) {
        return generate(workflow, awsRegion, awsAccountId).toJson();
    }

    /**
     * Convenience: generate the pretty-printed JSON form of the workflow's ASL definition.
     */
    public static String generatePrettyJson(Workflow workflow, String awsRegion, String awsAccountId) {
        return generate(workflow, awsRegion, awsAccountId).toPrettyJson();
    }

    private static String stepStateName(Class<? extends WorkflowStep> stepClass) {
        return stepClass.getSimpleName();
    }

    /**
     * The state name a transition into the given step should target. Partitioned steps are entered
     * through their dedicated generator state; regular steps are entered through their Task state.
     */
    private static String entryStateName(Class<? extends WorkflowStep> stepClass) {
        if (PartitionedWorkflowStep.class.isAssignableFrom(stepClass)) {
            return generatorStateName(stepClass);
        }
        return stepStateName(stepClass);
    }

    private static String choiceStateName(Class<? extends WorkflowStep> stepClass) {
        return "Branch" + stepClass.getSimpleName();
    }

    private static String generatorStateName(Class<? extends WorkflowStep> stepClass) {
        return stepClass.getSimpleName() + "PartitionGenerator";
    }

    /**
     * Builds an ASL Retry rule from the step's {@code @StepApply} annotation. The rule fires on the
     * special "retry" error code that {@link ActivityTaskPoller} reports back to Step Functions.
     */
    static RetryRule buildRetryRule(WorkflowStep step) {
        Method applyMethod = WorkflowStepUtil.getUniqueAnnotatedMethod(step.getClass(), StepApply.class);
        StepApply cfg = applyMethod.getAnnotation(StepApply.class);

        double backoffBase = cfg.exponentialBackoffBase();
        if (backoffBase <= 0.0) {
            backoffBase = 1.25;
        }

        String jitterStrategy = cfg.jitterPercent() > 0 ? RetryRule.JITTER_STRATEGY_FULL : null;
        return new RetryRule(ActivityTaskPoller.RETRY_ERROR_CODE, cfg.initialRetryDelaySeconds(),
                             99L, backoffBase, cfg.maxRetryDelaySeconds(), jitterStrategy);
    }
}
