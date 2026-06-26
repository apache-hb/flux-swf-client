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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.clients.sfn.AslExporter;
import com.danielgmyers.flux.clients.sfn.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.sfn.FluxCapacitorFactory;
import com.danielgmyers.flux.clients.sfn.SfnFluxCapacitor;
import com.danielgmyers.flux.clients.sfn.TestConfig;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.WorkflowStatus;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.danielgmyers.metrics.recorders.NoopMetricRecorderFactory;
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
import software.amazon.awssdk.services.sfn.model.StateMachineType;

/**
 * End-to-end integration test that simulates the CDK-managed deployment model:
 *
 * <ol>
 *   <li>Build-time: derive the ASL JSON for our workflow via {@link AslExporter}.</li>
 *   <li>"Deploy" the state machine and activities via the raw SFN SDK (the stand-in for CDK).</li>
 *   <li>Runtime: start a Flux capacitor configured with {@code registerWorkflowsOnStartup=false}, and
 *       confirm it can still execute the workflow end-to-end.</li>
 * </ol>
 *
 * <p>This is the integration test for the externally-managed-state-machine mode added to
 * {@code SfnFluxCapacitor}.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExternallyManagedWorkflowTest {
    private static final Logger log = LoggerFactory.getLogger(ExternallyManagedWorkflowTest.class);

    private static final List<Workflow> WORKFLOWS = List.of(new ExternallyManagedWorkflow());

    private SfnClient sfnClient;
    private SfnFluxCapacitor capacitor;

    @BeforeAll
    public void setUp() throws InterruptedException {
        sfnClient = SfnClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.of(TestConfig.getAwsRegion()))
                .build();

        // 1) Build-time: produce the ASL JSON via AslExporter — write it to a per-workflow stream
        // owned by the caller (here, an in-memory buffer that mirrors what a CDK build step would do
        // by piping into a local file).
        Workflow workflow = WORKFLOWS.get(0);
        ByteArrayOutputStream aslBuffer = new ByteArrayOutputStream();
        try {
            AslExporter.writeAslDefinition(workflow, TestConfig.getAwsRegion(),
                                            TestConfig.getAwsAccountId(), aslBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String aslDefinition = aslBuffer.toString(StandardCharsets.UTF_8);
        String stateMachineName = AslExporter.stateMachineName(workflow);
        Assertions.assertEquals("ExternallyManagedWorkflow", stateMachineName);

        // 2) "CDK deploy": create the activities and the state machine via the raw SDK.
        for (String activityName : AslExporter.listExpectedActivityNames(workflow)) {
            try {
                sfnClient.createActivity(CreateActivityRequest.builder().name(activityName).build());
            } catch (RuntimeException e) {
                // Idempotent: if it already exists from a previous run, that's fine.
                log.info("CreateActivity for {} returned: {}", activityName, e.getMessage());
            }
        }
        try {
            sfnClient.createStateMachine(CreateStateMachineRequest.builder()
                    .name(stateMachineName)
                    .definition(aslDefinition)
                    .roleArn(TestConfig.getStateMachineRoleArn())
                    .type(StateMachineType.STANDARD)
                    .publish(true)
                    .build());
        } catch (RuntimeException e) {
            log.info("CreateStateMachine returned: {} (likely already exists from a previous run)",
                     e.getMessage());
        }

        // 3) Runtime: spin up Flux with registration disabled.
        FluxCapacitorConfig config = TestConfig.generateFluxConfig(2);
        config.setRegisterWorkflowsOnStartup(false);
        capacitor = FluxCapacitorFactory.create(new NoopMetricRecorderFactory(),
                                                DefaultCredentialsProvider.create(),
                                                config);
        capacitor.initialize(WORKFLOWS);
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
    public void runtimeRunsWorkflowAgainstExternallyDeployedStateMachine() throws InterruptedException {
        String workflowId = "external-" + UUID.randomUUID();
        WorkflowStatusChecker checker = capacitor.executeWorkflow(
                ExternallyManagedWorkflow.class, workflowId,
                Map.of("payload", "hello-cdk"));

        // Poll for completion.
        Instant deadline = Instant.now().plus(Duration.ofMinutes(3));
        WorkflowStatus status;
        do {
            status = checker.checkStatus();
            if (status == WorkflowStatus.COMPLETED || status == WorkflowStatus.FAILED) {
                break;
            }
            TimeUnit.SECONDS.sleep(2);
        } while (Instant.now().isBefore(deadline));

        Assertions.assertEquals(WorkflowStatus.COMPLETED, status);
        Assertions.assertTrue(ExternallyManagedStep.didExecute(workflowId),
                              "Worker should have run the activity for the externally-managed workflow");
    }

    @Test
    public void aslExporterEmitsActivityNamesForCdkConsumption() {
        List<String> activityNames = AslExporter.listExpectedActivityNames(WORKFLOWS.get(0));
        Assertions.assertEquals(List.of("ExternallyManagedWorkflow-ExternallyManagedStep"), activityNames);
    }

    public static final class ExternallyManagedWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public ExternallyManagedWorkflow() {
            WorkflowStep step = new ExternallyManagedStep();
            this.graph = new WorkflowGraphBuilder(step, Map.of("payload", String.class))
                    .alwaysClose(step)
                    .build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static final class ExternallyManagedStep implements WorkflowStep {
        private static final Set<String> executedWorkflowIds = Collections.synchronizedSet(new HashSet<>());

        @StepApply
        public void doThing(@Attribute(StepAttributes.WORKFLOW_EXECUTION_ID) String workflowId,
                            @Attribute("payload") String payload) {
            executedWorkflowIds.add(workflowId);
        }

        static boolean didExecute(String workflowId) {
            return executedWorkflowIds.contains(workflowId);
        }
    }
}
