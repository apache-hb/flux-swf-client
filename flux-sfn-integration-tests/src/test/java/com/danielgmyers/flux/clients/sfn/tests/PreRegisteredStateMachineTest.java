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
import java.io.IOException;
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
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Exercises Flux's "state machine already registered with the desired definition" code path.
 *
 * <p>Setup deploys the state machine and activities directly via the raw SFN SDK using ASL JSON
 * produced by {@link AslExporter} — the same definition Flux would generate internally. The Flux
 * capacitor is then started with {@code registerWorkflowsOnStartup=true} (the default), so it
 * runs through {@code registerOrUpdateWorkflow}: {@code DescribeStateMachine} returns the existing
 * resource, {@code definitionMatches} compares the JSON, and the path short-circuits without calling
 * {@code UpdateStateMachine}.</p>
 *
 * <p>The assertion that no update occurred is structural: every {@code CreateStateMachine}/{@code
 * UpdateStateMachine} call with {@code publish=true} (which is how Flux deploys) produces a new
 * state machine version. We snapshot the version count after the pre-registration step and re-check
 * it after Flux has initialized; if Flux issued an update, the count would increase by one.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PreRegisteredStateMachineTest {
    private static final Logger log = LoggerFactory.getLogger(PreRegisteredStateMachineTest.class);

    private static final List<Workflow> WORKFLOWS = List.of(new PreRegisteredWorkflow());

    private SfnClient sfnClient;
    private SfnFluxCapacitor capacitor;
    private int preRegistrationVersionCount;

    @BeforeAll
    public void setUp() throws Exception {
        sfnClient = SfnClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.of(TestConfig.getAwsRegion()))
                .build();

        Workflow workflow = WORKFLOWS.get(0);
        String stateMachineName = AslExporter.stateMachineName(workflow);
        String stateMachineArn = SfnArnFormatter.workflowArn(TestConfig.getAwsRegion(),
                                                              TestConfig.getAwsAccountId(),
                                                              workflow.getClass());

        // 1) Produce the ASL JSON using the exact same generator Flux uses internally. That's the
        //    whole point of this test: Flux must recognise its own ASL output as "already registered"
        //    and skip the UpdateStateMachine call.
        ByteArrayOutputStream aslBuffer = new ByteArrayOutputStream();
        try {
            AslExporter.writeAslDefinition(workflow, TestConfig.getAwsRegion(),
                                            TestConfig.getAwsAccountId(), aslBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String aslDefinition = aslBuffer.toString(StandardCharsets.UTF_8);

        // 2) Pre-register the activities and the state machine via the raw SDK. If the state machine
        //    already exists from a previous test run with a different generated definition, update it
        //    so this run starts from a known state where Flux's definition matches what's deployed.
        for (String activityName : AslExporter.listExpectedActivityNames(workflow)) {
            try {
                sfnClient.createActivity(CreateActivityRequest.builder().name(activityName).build());
            } catch (RuntimeException e) {
                log.info("CreateActivity for {} returned: {}", activityName, e.getMessage());
            }
        }
        String existingDefinition = describeDefinition(stateMachineArn);
        if (existingDefinition == null) {
            sfnClient.createStateMachine(CreateStateMachineRequest.builder()
                    .name(stateMachineName)
                    .definition(aslDefinition)
                    .roleArn(TestConfig.getStateMachineRoleArn())
                    .type(StateMachineType.STANDARD)
                    .publish(true)
                    .build());
        } else if (!new ObjectMapper().readTree(existingDefinition)
                                      .equals(new ObjectMapper().readTree(aslDefinition))) {
            sfnClient.updateStateMachine(UpdateStateMachineRequest.builder()
                    .stateMachineArn(stateMachineArn)
                    .definition(aslDefinition)
                    .roleArn(TestConfig.getStateMachineRoleArn())
                    .publish(true)
                    .build());
        }

        // 3) Snapshot the published-version count. Every CreateStateMachine / UpdateStateMachine
        //    call with publish=true (the mode Flux uses) appends a new entry here, so this is a
        //    stable signal for whether Flux's initialize() ended up calling UpdateStateMachine.
        preRegistrationVersionCount = countStateMachineVersions(stateMachineArn);
        Assertions.assertTrue(preRegistrationVersionCount >= 1,
                              "Pre-registered state machine should have at least one published version");
        log.info("Pre-registered state machine {} has {} version(s)",
                 stateMachineName, preRegistrationVersionCount);

        // 4) Start Flux with the default registerWorkflowsOnStartup=true. The interesting code path
        //    here is registerOrUpdateWorkflow → describeStateMachine → definitionMatches=true
        //    → return without touching the state machine.
        FluxCapacitorConfig config = TestConfig.generateFluxConfig(1);
        capacitor = FluxCapacitorFactory.create(new NoopMetricRecorderFactory(),
                                                DefaultCredentialsProvider.create(),
                                                config);
        capacitor.initialize(WORKFLOWS);

        // 5) The version count must not change as a result of initialize().
        int postInitVersionCount = countStateMachineVersions(stateMachineArn);
        Assertions.assertEquals(preRegistrationVersionCount, postInitVersionCount,
                                "Flux must not publish a new version when the deployed definition already matches");
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
    public void runtimeExecutesWorkflowAgainstPreRegisteredStateMachine() throws InterruptedException {
        String workflowId = "preregistered-" + UUID.randomUUID();

        WorkflowStatusChecker checker = capacitor.executeWorkflow(
                PreRegisteredWorkflow.class, workflowId, Collections.emptyMap());

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
        Assertions.assertTrue(PreRegisteredStep.didExecute(workflowId),
                              "Activity worker should have run against the pre-registered state machine");

        // Re-confirm at the end of the test that the state machine still hasn't been updated.
        String stateMachineArn = SfnArnFormatter.workflowArn(TestConfig.getAwsRegion(),
                                                              TestConfig.getAwsAccountId(),
                                                              PreRegisteredWorkflow.class);
        Assertions.assertEquals(preRegistrationVersionCount, countStateMachineVersions(stateMachineArn),
                                "Running an execution must not publish a new state machine version");
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

    private int countStateMachineVersions(String stateMachineArn) {
        try {
            int total = 0;
            String token = null;
            do {
                ListStateMachineVersionsResponse resp = sfnClient.listStateMachineVersions(
                        ListStateMachineVersionsRequest.builder()
                                .stateMachineArn(stateMachineArn)
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

    public static final class PreRegisteredWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public PreRegisteredWorkflow() {
            WorkflowStep step = new PreRegisteredStep();
            this.graph = new WorkflowGraphBuilder(step).alwaysClose(step).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static final class PreRegisteredStep implements WorkflowStep {
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
