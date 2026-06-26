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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.PartitionIdGenerator;
import com.danielgmyers.flux.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepHook;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.WorkflowStepHook;
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
 * End-to-end integration test exercising a workflow that combines partitioning, step hooks,
 * branching transitions, retries, and attribute passing between steps.
 *
 * <p>The workflow models a small order-processing pipeline:</p>
 * <pre>
 *   ValidateOrder ─_succeed→ ChargeEachItem ─_succeed→ SendConfirmation (close)
 *                 └_fail────→ RejectOrder (close)
 * </pre>
 *
 * <p>{@code ChargeEachItem} is a partitioned step — the partition-id generator builds one partition
 * per line-item, each partition retries once before succeeding, and the workflow only proceeds to
 * {@code SendConfirmation} after every partition has succeeded.</p>
 *
 * <p>{@code AuditHook} is attached to all steps (pre + post) and records that the hooks fired,
 * proving step hooks run in the SFN backend just as they do in SWF.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ComplexWorkflowTest extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(ComplexWorkflowTest.class);

    /** Number of partitions ChargeEachItem.generatePartitionIds() should produce. */
    static final int PARTITIONS_PER_ORDER = 4;

    /** Number of retries each partition should perform before succeeding. */
    static final int RETRIES_PER_PARTITION = 1;

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new OrderWorkflow());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    @Override
    protected int getWorkerPoolThreadCount() {
        // ValidateOrder, ChargeEachItem (generator), ChargeEachItem (per partition), RejectOrder, SendConfirmation —
        // plus parallel partition iterations. Keep the pool roomy so we don't starve any single activity poller.
        return 10;
    }


    @Test
    public void successPathRunsAllStepsAndAllPartitionsWithHooksAndAttributePassing() throws InterruptedException {
        String workflowId = "order-success-" + UUID.randomUUID();
        Map<String, Object> input = Map.of(
                "orderId", workflowId,
                "shouldSucceed", Boolean.TRUE,
                "customerEmail", "alice@example.com");

        WorkflowStatusChecker checker = executeWorkflow(OrderWorkflow.class, workflowId, input);
        WorkflowStatus status = waitForWorkflowCompletion(checker, Duration.ofMinutes(10));

        Assertions.assertEquals(WorkflowStatus.COMPLETED, status, "Order workflow should succeed end-to-end");

        // ValidateOrder ran exactly once.
        Assertions.assertEquals(1, ValidateOrder.invocationsForOrder(workflowId),
                                "ValidateOrder should run exactly once for " + workflowId);

        // The partition-id generator was invoked for the partitioned step.
        Assertions.assertEquals(1, ChargeEachItem.generatorInvocationsForOrder(workflowId),
                                "The partition-id generator should be invoked once");

        // Every partition ran (and retried RETRIES_PER_PARTITION times before succeeding).
        Set<String> partitionsRun = ChargeEachItem.partitionsExecutedForOrder(workflowId);
        Assertions.assertEquals(PARTITIONS_PER_ORDER, partitionsRun.size(),
                                "Every partition should have been executed exactly once on the success path");

        // Attribute passing: ValidateOrder put `validatedAt`/`itemCount` into output, which the
        // partition-generator and partition activities should observe.
        Assertions.assertEquals(PARTITIONS_PER_ORDER, ChargeEachItem.lastItemCountSeenForOrder(workflowId),
                                "Generator should see the validated item count via attribute passing");
        Assertions.assertTrue(ChargeEachItem.allPartitionsSawValidatedAt(workflowId),
                              "Every partition should see the `validatedAt` attribute produced by ValidateOrder");

        // SendConfirmation ran (success-branch terminal).
        Assertions.assertEquals("alice@example.com", SendConfirmation.lastEmailForOrder(workflowId),
                                "SendConfirmation should run on the success branch and see the customerEmail attribute");

        // RejectOrder did NOT run.
        Assertions.assertEquals(0, RejectOrder.invocationsForOrder(workflowId),
                                "RejectOrder should not run on the success branch");

        // Step hooks fired for every step in the success path.
        Assertions.assertTrue(AuditHook.sawPreFor(workflowId, "ValidateOrder"));
        Assertions.assertTrue(AuditHook.sawPostFor(workflowId, "ValidateOrder"));
        Assertions.assertTrue(AuditHook.sawPreFor(workflowId, "SendConfirmation"));
        Assertions.assertTrue(AuditHook.sawPostFor(workflowId, "SendConfirmation"));

        // Retries: each partition retried RETRIES_PER_PARTITION times.
        int totalAttempts = ChargeEachItem.totalPartitionAttemptsForOrder(workflowId);
        Assertions.assertEquals(PARTITIONS_PER_ORDER * (1 + RETRIES_PER_PARTITION), totalAttempts,
                                "Each partition should make (1 + RETRIES_PER_PARTITION) attempts");
    }

    @Test
    public void failurePathBranchesToRejectAndSkipsPartitionedAndConfirmationSteps() throws InterruptedException {
        String workflowId = "order-failure-" + UUID.randomUUID();
        Map<String, Object> input = Map.of(
                "orderId", workflowId,
                "shouldSucceed", Boolean.FALSE,
                "customerEmail", "bob@example.com");

        WorkflowStatusChecker checker = executeWorkflow(OrderWorkflow.class, workflowId, input);
        WorkflowStatus status = waitForWorkflowCompletion(checker, Duration.ofMinutes(10));

        Assertions.assertEquals(WorkflowStatus.COMPLETED, status);

        Assertions.assertEquals(1, ValidateOrder.invocationsForOrder(workflowId));
        Assertions.assertEquals(1, RejectOrder.invocationsForOrder(workflowId),
                                "RejectOrder should run on the failure branch");
        Assertions.assertEquals(0, ChargeEachItem.generatorInvocationsForOrder(workflowId),
                                "The partitioned step's generator should be skipped on the failure branch");
        Assertions.assertTrue(ChargeEachItem.partitionsExecutedForOrder(workflowId).isEmpty(),
                              "No partitions should run on the failure branch");
        Assertions.assertNull(SendConfirmation.lastEmailForOrder(workflowId),
                              "SendConfirmation should not run on the failure branch");

        // Hooks fired for steps that actually ran.
        Assertions.assertTrue(AuditHook.sawPreFor(workflowId, "ValidateOrder"));
        Assertions.assertTrue(AuditHook.sawPostFor(workflowId, "ValidateOrder"));
        Assertions.assertTrue(AuditHook.sawPreFor(workflowId, "RejectOrder"));
        Assertions.assertTrue(AuditHook.sawPostFor(workflowId, "RejectOrder"));
        Assertions.assertFalse(AuditHook.sawPreFor(workflowId, "SendConfirmation"));
    }

    // =====================================================================================
    // Workflow definition
    // =====================================================================================

    public static final class OrderWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public OrderWorkflow() {
            ValidateOrder validate = new ValidateOrder();
            ChargeEachItem charge = new ChargeEachItem();
            SendConfirmation confirm = new SendConfirmation();
            RejectOrder reject = new RejectOrder();

            Map<String, Class<?>> initialAttrs = new HashMap<>();
            initialAttrs.put("orderId", String.class);
            initialAttrs.put("shouldSucceed", Boolean.class);
            initialAttrs.put("customerEmail", String.class);

            this.graph = new WorkflowGraphBuilder(validate, initialAttrs)
                    .successTransition(validate, charge)
                    .failTransition(validate, reject)
                    .addStep(charge).alwaysTransition(charge, confirm)
                    .addStep(confirm).alwaysClose(confirm)
                    .addStep(reject).alwaysClose(reject)
                    .addHookForAllSteps(new AuditHook())
                    .build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    // =====================================================================================
    // Steps
    // =====================================================================================

    /**
     * Decides whether the order is valid. Emits two output attributes ({@code validatedAt},
     * {@code itemCount}) that downstream steps depend on.
     */
    public static final class ValidateOrder implements WorkflowStep {
        private static final Map<String, AtomicInteger> invocations = new ConcurrentHashMap<>();

        @Override
        public Map<String, Class<?>> declaredOutputAttributes() {
            // Declared so the graph builder validates that downstream steps may consume them.
            return Map.of(
                    "validatedAt", Long.class,
                    "itemCount", Long.class);
        }

        @StepApply
        public StepResult validate(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId,
                                   @Attribute("shouldSucceed") Boolean shouldSucceed,
                                   @Attribute("orderId") String orderId) {
            invocations.computeIfAbsent(workflowId, w -> new AtomicInteger()).incrementAndGet();
            if (Boolean.TRUE.equals(shouldSucceed)) {
                return StepResult.success("validated order " + orderId)
                        .withAttribute("validatedAt", (long) PARTITIONS_PER_ORDER * 1000L)
                        .withAttribute("itemCount", (long) PARTITIONS_PER_ORDER);
            }
            return StepResult.failure("order " + orderId + " is invalid");
        }

        static int invocationsForOrder(String workflowId) {
            AtomicInteger n = invocations.get(workflowId);
            return n == null ? 0 : n.get();
        }
    }

    /**
     * Partitioned step that "charges" each line-item. The generator returns one partition per item
     * (where {@code itemCount} came from ValidateOrder, demonstrating attribute passing). Each partition
     * fails on its first attempt and succeeds on the second, exercising the ASL Retry rule.
     */
    public static final class ChargeEachItem implements PartitionedWorkflowStep {
        private static final Map<String, AtomicInteger> generatorInvocations = new ConcurrentHashMap<>();
        private static final Map<String, Set<String>> partitionsExecuted = new ConcurrentHashMap<>();
        private static final Map<String, AtomicInteger> totalPartitionAttempts = new ConcurrentHashMap<>();
        private static final Map<String, AtomicInteger> lastItemCountSeen = new ConcurrentHashMap<>();
        private static final Map<String, Set<String>> partitionsThatSawValidatedAt = new ConcurrentHashMap<>();
        // attempt counter for each (workflowId, partitionId) to drive the retry-then-succeed behavior.
        private static final Map<String, AtomicInteger> attemptsByOrderAndPartition = new ConcurrentHashMap<>();

        @PartitionIdGenerator
        public PartitionIdGeneratorResult generatePartitionIds(
                @Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId,
                @Attribute("itemCount") Long itemCount,
                @Attribute("validatedAt") Long validatedAt) {
            generatorInvocations.computeIfAbsent(workflowId, w -> new AtomicInteger()).incrementAndGet();
            lastItemCountSeen.computeIfAbsent(workflowId, w -> new AtomicInteger()).set(itemCount.intValue());
            partitionsExecuted.putIfAbsent(workflowId, ConcurrentHashMap.newKeySet());
            partitionsThatSawValidatedAt.putIfAbsent(workflowId, ConcurrentHashMap.newKeySet());

            Set<String> ids = new HashSet<>();
            for (int i = 0; i < itemCount; i++) {
                ids.add("item-" + i);
            }
            return PartitionIdGeneratorResult.create(ids);
        }

        @StepApply(initialRetryDelaySeconds = 1, maxRetryDelaySeconds = 1,
                   retriesBeforeBackoff = 5, jitterPercent = 0)
        public StepResult charge(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId,
                                 @Attribute(StepAttributes.PARTITION_ID) String partitionId,
                                 @Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount,
                                 @Attribute("validatedAt") Long validatedAt) {
            // Track that we saw the upstream-produced attribute.
            if (validatedAt != null) {
                partitionsThatSawValidatedAt
                        .computeIfAbsent(workflowId, w -> ConcurrentHashMap.newKeySet())
                        .add(partitionId);
            }
            totalPartitionAttempts.computeIfAbsent(workflowId, w -> new AtomicInteger()).incrementAndGet();

            String attemptKey = workflowId + "::" + partitionId;
            int attemptNumber = attemptsByOrderAndPartition
                    .computeIfAbsent(attemptKey, k -> new AtomicInteger())
                    .incrementAndGet();
            if (attemptNumber <= RETRIES_PER_PARTITION) {
                return StepResult.retry("retrying partition " + partitionId + " (attempt " + attemptNumber + ")");
            }

            partitionsExecuted
                    .computeIfAbsent(workflowId, w -> ConcurrentHashMap.newKeySet())
                    .add(partitionId);
            return StepResult.success("charged " + partitionId + " for partition count " + partitionCount);
        }

        static int generatorInvocationsForOrder(String workflowId) {
            AtomicInteger n = generatorInvocations.get(workflowId);
            return n == null ? 0 : n.get();
        }

        static Set<String> partitionsExecutedForOrder(String workflowId) {
            return partitionsExecuted.getOrDefault(workflowId, Collections.emptySet());
        }

        static int totalPartitionAttemptsForOrder(String workflowId) {
            AtomicInteger n = totalPartitionAttempts.get(workflowId);
            return n == null ? 0 : n.get();
        }

        static int lastItemCountSeenForOrder(String workflowId) {
            AtomicInteger n = lastItemCountSeen.get(workflowId);
            return n == null ? 0 : n.get();
        }

        static boolean allPartitionsSawValidatedAt(String workflowId) {
            Set<String> sawIt = partitionsThatSawValidatedAt.getOrDefault(workflowId, Collections.emptySet());
            return sawIt.size() == PARTITIONS_PER_ORDER;
        }
    }

    /** Terminal step on the success branch. */
    public static final class SendConfirmation implements WorkflowStep {
        private static final Map<String, String> lastEmailByOrder = new ConcurrentHashMap<>();

        @StepApply
        public void send(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId,
                         @Attribute("customerEmail") String email) {
            lastEmailByOrder.put(workflowId, email);
        }

        static String lastEmailForOrder(String workflowId) {
            return lastEmailByOrder.get(workflowId);
        }
    }

    /** Terminal step on the failure branch. */
    public static final class RejectOrder implements WorkflowStep {
        private static final Map<String, AtomicInteger> invocations = new ConcurrentHashMap<>();

        @StepApply
        public void reject(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId,
                           @Attribute("orderId") String orderId) {
            invocations.computeIfAbsent(workflowId, w -> new AtomicInteger()).incrementAndGet();
        }

        static int invocationsForOrder(String workflowId) {
            AtomicInteger n = invocations.get(workflowId);
            return n == null ? 0 : n.get();
        }
    }

    // =====================================================================================
    // Hooks
    // =====================================================================================

    /** Records PRE/POST step-hook invocations keyed by (workflowId, activityName). */
    public static final class AuditHook implements WorkflowStepHook {
        private static final Set<String> preCalls = ConcurrentHashMap.newKeySet();
        private static final Set<String> postCalls = ConcurrentHashMap.newKeySet();

        @StepHook(hookType = StepHook.HookType.PRE)
        public void onPre(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId,
                          @Attribute(StepAttributes.ACTIVITY_NAME) String activityName) {
            preCalls.add(key(workflowId, activityName));
        }

        @StepHook(hookType = StepHook.HookType.POST)
        public void onPost(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId,
                           @Attribute(StepAttributes.ACTIVITY_NAME) String activityName) {
            postCalls.add(key(workflowId, activityName));
        }

        static boolean sawPreFor(String workflowId, String stepSimpleName) {
            return matchesAny(preCalls, workflowId, stepSimpleName);
        }

        static boolean sawPostFor(String workflowId, String stepSimpleName) {
            return matchesAny(postCalls, workflowId, stepSimpleName);
        }

        // The ACTIVITY_NAME attribute is delivered to hooks JSON-encoded (i.e. as `"OrderWorkflow.StepName"`
        // with surrounding quotes); accept either form to insulate the test from that quirk.
        private static boolean matchesAny(Set<String> calls, String workflowId, String stepSimpleName) {
            String prefix = workflowId + "::";
            return calls.stream().anyMatch(k -> k.startsWith(prefix)
                    && (k.endsWith("." + stepSimpleName) || k.endsWith("." + stepSimpleName + "\"")));
        }


        private static String key(String workflowId, String activityName) {
            return workflowId + "::" + activityName;
        }
    }
}
