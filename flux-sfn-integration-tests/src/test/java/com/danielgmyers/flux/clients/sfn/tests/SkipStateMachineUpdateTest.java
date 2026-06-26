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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.clients.sfn.AslExporter;
import com.danielgmyers.flux.clients.sfn.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.sfn.FluxCapacitorFactory;
import com.danielgmyers.flux.clients.sfn.SfnFluxCapacitor;
import com.danielgmyers.flux.clients.sfn.TestConfig;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.WorkflowStatus;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.danielgmyers.metrics.recorders.NoopMetricRecorderFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.CreateActivityRequest;
import software.amazon.awssdk.services.sfn.model.CreateStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.DescribeStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.DescribeStateMachineResponse;
import software.amazon.awssdk.services.sfn.model.ListStateMachineVersionsRequest;
import software.amazon.awssdk.services.sfn.model.ListStateMachineVersionsResponse;
import software.amazon.awssdk.services.sfn.model.StateMachineDoesNotExistException;
import software.amazon.awssdk.services.sfn.model.StateMachineType;
import software.amazon.awssdk.services.sfn.model.StateMachineVersionListItem;
import software.amazon.awssdk.services.sfn.model.UpdateStateMachineRequest;

/**
 * Covers the {@link FluxCapacitorConfig#setUpdateExistingStateMachines(boolean)} option.
 *
 * <p>Setup deploys an ASL definition that diverges from what Flux would generate (a tweaked
 * top-level {@code Comment}) so the {@code definitionMatches} check fails. The Flux capacitor is
 * then initialized with {@code updateExistingStateMachines=false}, which must cause Flux to
 * recognise the mismatch and skip {@code UpdateStateMachine} rather than overwriting the deployed
 * definition.</p>
 *
 * <p>The deployed definition is otherwise byte-identical to Flux's output (only the Comment field
 * differs), so workflows still execute end-to-end — proving Flux can poll for and dispatch
 * activities against a state machine it didn't author.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SkipStateMachineUpdateTest {
    private static final Logger log = LoggerFactory.getLogger(SkipStateMachineUpdateTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String EXTERNAL_COMMENT = "Externally managed by SkipStateMachineUpdateTest";

    private static final List<Workflow> WORKFLOWS = List.of(new SkipUpdateWorkflow());

    private SfnClient sfnClient;
    private SfnFluxCapacitor capacitor;
    private int preInitVersionCount;
    private String preInitDefinition;
    private String stateMachineArn;

    @BeforeAll
    public void setUp() throws Exception {
        sfnClient = SfnClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.of(TestConfig.getAwsRegion()))
                .build();

        Workflow workflow = WORKFLOWS.get(0);
        String stateMachineName = AslExporter.stateMachineName(workflow);
        stateMachineArn = SfnArnFormatter.workflowArn(TestConfig.getAwsRegion(),
                                                     TestConfig.getAwsAccountId(),
                                                     workflow.getClass());

        // 1) Produce a tweaked ASL definition: Flux's output with a different top-level Comment so
        //    definitionMatches() sees a mismatch and would normally call UpdateStateMachine.
        String tweakedAsl = tweakCommentOnAsl(workflow);

        // 2) Make sure the activities exist (Flux still talks to those at runtime).
        for (String activityName : AslExporter.listExpectedActivityNames(workflow)) {
            try {
                sfnClient.createActivity(CreateActivityRequest.builder().name(activityName).build());
            } catch (RuntimeException e) {
                log.info("CreateActivity for {} returned: {}", activityName, e.getMessage());
            }
        }

        // 3) Deploy (or reset to) the tweaked definition. If the state machine survived from a prior
        //    run with a different definition, force-update it so we have a known starting state for
        //    the version-count assertion below.
        String existingDefinition = describeDefinition(stateMachineArn);
        if (existingDefinition == null) {
            sfnClient.createStateMachine(CreateStateMachineRequest.builder()
                    .name(stateMachineName)
                    .definition(tweakedAsl)
                    .roleArn(TestConfig.getStateMachineRoleArn())
                    .type(StateMachineType.STANDARD)
                    .publish(true)
                    .build());
        } else if (!MAPPER.readTree(existingDefinition).equals(MAPPER.readTree(tweakedAsl))) {
            sfnClient.updateStateMachine(UpdateStateMachineRequest.builder()
                    .stateMachineArn(stateMachineArn)
                    .definition(tweakedAsl)
                    .roleArn(TestConfig.getStateMachineRoleArn())
                    .publish(true)
                    .build());
        }

        // 4) Capture the state machine's version count and definition just before Flux initializes,
        //    so we can prove Flux didn't touch it.
        preInitVersionCount = countStateMachineVersions(stateMachineArn);
        preInitDefinition = describeDefinition(stateMachineArn);
        Assertions.assertNotNull(preInitDefinition);
        Assertions.assertTrue(preInitDefinition.contains(EXTERNAL_COMMENT),
                              "Pre-init definition should be the externally-managed variant");
        log.info("Pre-init state of {}: {} version(s)", stateMachineName, preInitVersionCount);

        // 5) Start Flux with registerWorkflowsOnStartup=true (so it WILL hit the
        //    register-or-update path) but updateExistingStateMachines=false. The new branch should
        //    log a warning and skip UpdateStateMachine.
        FluxCapacitorConfig config = TestConfig.generateFluxConfig(1);
        config.setUpdateExistingStateMachines(false);
        capacitor = FluxCapacitorFactory.create(new NoopMetricRecorderFactory(),
                                                DefaultCredentialsProvider.create(),
                                                config);
        capacitor.initialize(WORKFLOWS);

        // 6) Confirm the version count did not change.
        int postInitVersionCount = countStateMachineVersions(stateMachineArn);
        Assertions.assertEquals(preInitVersionCount, postInitVersionCount,
                                "Flux must not publish a new state machine version when "
                                + "updateExistingStateMachines=false");
        // The deployed definition must still be the externally-authored one.
        String postInitDefinition = describeDefinition(stateMachineArn);
        Assertions.assertEquals(MAPPER.readTree(preInitDefinition),
                                MAPPER.readTree(postInitDefinition),
                                "Flux must not rewrite the deployed definition");
        Assertions.assertTrue(postInitDefinition.contains(EXTERNAL_COMMENT),
                              "Externally-set Comment should still be present after initialize()");
    }

    @AfterAll
    public void tearDown() throws InterruptedException {
        if (capacitor != null) {
            capacitor.shutdown();
            capacitor.awaitTermination(30, TimeUnit.SECONDS);
        }
        if (sfnClient != null) {
            sfnClient.close();
        }
        // Intentionally NOT deleting the state machine/activities so the user can inspect them.
    }

    @Test
    public void runtimeRunsWorkflowAgainstDivergentStateMachine() throws Exception {
        String workflowId = "skip-update-" + UUID.randomUUID();

        WorkflowStatusChecker checker = capacitor.executeWorkflow(
                SkipUpdateWorkflow.class, workflowId, Collections.emptyMap());

        WorkflowStatus status;
        Instant deadline = Instant.now().plus(Duration.ofMinutes(3));
        do {
            status = checker.checkStatus();
            if (status == WorkflowStatus.COMPLETED || status == WorkflowStatus.FAILED) {
                break;
            }
            TimeUnit.SECONDS.sleep(2);
        } while (Instant.now().isBefore(deadline));

        Assertions.assertEquals(WorkflowStatus.COMPLETED, status);
        Assertions.assertTrue(SkipUpdateStep.didExecute(workflowId),
                              "Activity worker should run against the divergent state machine");

        // After running an execution, the deployed definition still must not have been replaced —
        // re-check version count and Comment one more time.
        Assertions.assertEquals(preInitVersionCount, countStateMachineVersions(stateMachineArn),
                                "Running an execution must not publish a new state machine version");
        String currentDefinition = describeDefinition(stateMachineArn);
        Assertions.assertEquals(MAPPER.readTree(preInitDefinition), MAPPER.readTree(currentDefinition));
        Assertions.assertTrue(currentDefinition.contains(EXTERNAL_COMMENT));
    }

    private String tweakCommentOnAsl(Workflow workflow) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        AslExporter.writeAslDefinition(workflow, TestConfig.getAwsRegion(),
                                        TestConfig.getAwsAccountId(), buffer);
        String fluxAsl = buffer.toString(StandardCharsets.UTF_8);
        ObjectNode root = (ObjectNode) MAPPER.readTree(fluxAsl);
        root.put("Comment", EXTERNAL_COMMENT);
        return MAPPER.writeValueAsString(root);
    }

    private int countStateMachineVersions(String arn) {
        try {
            int total = 0;
            String token = null;
            do {
                ListStateMachineVersionsResponse resp = sfnClient.listStateMachineVersions(
                        ListStateMachineVersionsRequest.builder()
                                .stateMachineArn(arn)
                                .nextToken(token)
                                .build());
                for (StateMachineVersionListItem item : resp.stateMachineVersions()) {
                    if (item != null) {
                        total++;
                    }
                }
                token = resp.nextToken();
            } while (token != null);
            return total;
        } catch (StateMachineDoesNotExistException e) {
            return 0;
        }
    }

    private String describeDefinition(String arn) {
        try {
            DescribeStateMachineResponse resp = sfnClient.describeStateMachine(
                    DescribeStateMachineRequest.builder().stateMachineArn(arn).build());
            return resp.definition();
        } catch (StateMachineDoesNotExistException e) {
            return null;
        }
    }

    public static final class SkipUpdateWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public SkipUpdateWorkflow() {
            WorkflowStep step = new SkipUpdateStep();
            this.graph = new WorkflowGraphBuilder(step).alwaysClose(step).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static final class SkipUpdateStep implements WorkflowStep {
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
