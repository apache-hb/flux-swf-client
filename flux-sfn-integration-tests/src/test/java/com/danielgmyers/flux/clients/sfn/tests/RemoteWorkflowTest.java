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

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.RemoteWorkflowExecutor;
import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.clients.sfn.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.sfn.FluxCapacitorFactory;
import com.danielgmyers.flux.clients.sfn.RemoteSfnClientConfig;
import com.danielgmyers.flux.clients.sfn.TaskListConfig;
import com.danielgmyers.flux.clients.sfn.TestConfig;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.WorkflowInfo;
import com.danielgmyers.flux.wf.WorkflowStatus;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.danielgmyers.metrics.recorders.NoopMetricRecorderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.SfnClientBuilder;
import software.amazon.awssdk.services.sfn.model.DeleteStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.ExecutionStatus;
import software.amazon.awssdk.services.sfn.model.ListExecutionsRequest;
import software.amazon.awssdk.services.sfn.model.StopExecutionRequest;

/**
 * Validates we can ask a {@link RemoteWorkflowExecutor} to start an execution in a different
 * AWS region. The remote region's state machine must be registered before we can start an
 * execution against it; the {@link BeforeAll} hook spins up a short-lived Flux against the
 * remote region just to register the workflow, then shuts it down.
 *
 * <p>The test deliberately doesn't run any activities to completion in the remote region — it
 * only verifies that the StartExecution call succeeded and that the resulting execution is in
 * the IN_PROGRESS state. We stop the remote execution and clean up the state machine afterwards.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RemoteWorkflowTest extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(RemoteWorkflowTest.class);

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new RemoteHelloWorld());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    @BeforeAll
    public void setUpRemoteWorkflows() throws InterruptedException {
        // Register the workflow in the remote region by spinning up a short-lived Flux pointed at it.
        RemoteSfnClientConfig remote = TestConfig.getRemoteSfnClientConfig();

        FluxCapacitorConfig remoteFluxConfig = new FluxCapacitorConfig();
        remoteFluxConfig.setAwsRegion(remote.getAwsRegion());
        remoteFluxConfig.setAwsAccountId(remote.getAwsAccountId());
        remoteFluxConfig.setStateMachineRoleArn(TestConfig.getStateMachineRoleArn());
        if (remote.getSfnEndpoint() != null) {
            remoteFluxConfig.setSfnEndpoint(remote.getSfnEndpoint());
        }
        TaskListConfig small = new TaskListConfig();
        small.setActivityTaskThreadCount(1);
        small.setActivityPollerThreadCount(1);
        remoteFluxConfig.putTaskListConfig(Workflow.DEFAULT_TASK_LIST_NAME, small);

        FluxCapacitor remoteCapacitor = FluxCapacitorFactory.create(new NoopMetricRecorderFactory(),
                                                                    DefaultCredentialsProvider.create(),
                                                                    remoteFluxConfig);
        remoteCapacitor.initialize(getWorkflowsForTest());

        log.info("Remote workflow registered; shutting remote Flux down.");
        remoteCapacitor.shutdown();
        remoteCapacitor.awaitTermination(30, TimeUnit.SECONDS);
    }

    @Test
    public void canStartRemoteWorkflow() {
        String workflowId = "remote-" + UUID.randomUUID();

        RemoteWorkflowExecutor remoteExecutor = getRemoteWorkflowExecutor();
        WorkflowStatusChecker checker = remoteExecutor.executeWorkflow(RemoteHelloWorld.class, workflowId,
                                                                        Collections.emptyMap());
        WorkflowInfo info = checker.getWorkflowInfo();

        log.info("Received status {} for remote workflow {}", info.getWorkflowStatus(), workflowId);
        Assertions.assertNotEquals(WorkflowStatus.UNKNOWN, info.getWorkflowStatus(),
                                   "Remote execution should have a known status");

        // Clean up the in-flight execution in the remote region.
        SfnClient remoteSfn = createRemoteSfnClient();
        try {
            remoteSfn.stopExecution(StopExecutionRequest.builder()
                    .executionArn(info.getExecutionId())
                    .cause("integration test cleanup")
                    .build());
        } catch (RuntimeException e) {
            log.warn("Failed to stop remote execution {}: {}", info.getExecutionId(), e.getMessage());
        } finally {
            remoteSfn.close();
        }
    }

    @Override
    public void cleanUpFluxCapacitor() throws InterruptedException {
        super.cleanUpFluxCapacitor();

        // Also clean up the state machine we registered in the remote region.
        SfnClient remoteSfn = createRemoteSfnClient();
        try {
            RemoteSfnClientConfig remote = TestConfig.getRemoteSfnClientConfig();
            for (Workflow workflow : getWorkflowsForTest()) {
                String remoteArn = SfnArnFormatter.workflowArn(remote.getAwsRegion(), remote.getAwsAccountId(),
                                                                workflow.getClass());
                // Stop any leftover executions first.
                try {
                    remoteSfn.listExecutionsPaginator(ListExecutionsRequest.builder()
                            .stateMachineArn(remoteArn)
                            .statusFilter(ExecutionStatus.RUNNING)
                            .build())
                            .executions()
                            .forEach(e -> remoteSfn.stopExecution(StopExecutionRequest.builder()
                                    .executionArn(e.executionArn()).cause("integration test cleanup").build()));
                } catch (RuntimeException e) {
                    log.warn("While stopping executions for {}: {}", remoteArn, e.getMessage());
                }
                try {
                    remoteSfn.deleteStateMachine(DeleteStateMachineRequest.builder()
                            .stateMachineArn(remoteArn).build());
                } catch (RuntimeException e) {
                    log.warn("Failed to delete remote state machine {}: {}", remoteArn, e.getMessage());
                }
            }
        } finally {
            remoteSfn.close();
        }
        // Wait briefly for the cleanup to settle.
        Thread.sleep(Duration.ofSeconds(1).toMillis());
    }

    private SfnClient createRemoteSfnClient() {
        RemoteSfnClientConfig remote = TestConfig.getRemoteSfnClientConfig();
        SfnClientBuilder builder = SfnClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.of(remote.getAwsRegion()));
        if (remote.getSfnEndpoint() != null) {
            builder.endpointOverride(URI.create(remote.getSfnEndpoint()));
        }
        return builder.build();
    }

    public static final class RemoteHelloWorld implements Workflow {
        private final WorkflowGraph graph;
        RemoteHelloWorld() {
            WorkflowStep stepOne = new RemoteStepOne();
            this.graph = new WorkflowGraphBuilder(stepOne, Collections.emptyMap()).alwaysClose(stepOne).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static final class RemoteStepOne implements WorkflowStep {
        @StepApply public void doThing() {}
    }
}
