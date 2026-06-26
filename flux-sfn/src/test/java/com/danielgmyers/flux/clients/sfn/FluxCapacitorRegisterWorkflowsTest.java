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

import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.danielgmyers.flux.clients.sfn.asl.AslGenerator;
import com.danielgmyers.flux.ex.FluxException;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.WorkflowStep;
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
import software.amazon.awssdk.services.sfn.model.CreateStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.CreateStateMachineResponse;
import software.amazon.awssdk.services.sfn.model.DescribeStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.DescribeStateMachineResponse;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskRequest;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskResponse;
import software.amazon.awssdk.services.sfn.model.StateMachineDoesNotExistException;
import software.amazon.awssdk.services.sfn.model.StateMachineType;
import software.amazon.awssdk.services.sfn.model.UpdateStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.UpdateStateMachineResponse;

public class FluxCapacitorRegisterWorkflowsTest {

    private static final String REGION = "us-east-1";
    private static final String ACCOUNT = "123456789012";
    private static final String ROLE = "arn:aws:iam::123456789012:role/sfn-role";

    public static class StepOne implements WorkflowStep {
        @StepApply public void apply() {}
    }

    public static class StepTwo implements WorkflowStep {
        @StepApply public void apply() {}
    }

    public static class TestWorkflow implements Workflow {
        private final WorkflowGraph graph;
        public TestWorkflow() {
            StepOne one = new StepOne();
            StepTwo two = new StepTwo();
            this.graph = new WorkflowGraphBuilder(one)
                    .alwaysTransition(one, two)
                    .addStep(two).alwaysClose(two)
                    .build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    private IMocksControl mockery;
    private SfnClient sfn;
    private FluxCapacitorConfig config;
    private FluxCapacitorImpl fc;

    @BeforeEach
    public void setup() {
        mockery = EasyMock.createControl();
        sfn = mockery.createMock(SfnClient.class);

        config = new FluxCapacitorConfig();
        config.setAwsRegion(REGION);
        config.setAwsAccountId(ACCOUNT);
        config.setStateMachineRoleArn(ROLE);

        fc = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), sfn, config, Clock.systemUTC());
    }

    @Test
    public void registersStateMachineWhenItDoesNotExist() {
        fc.populateMaps(List.of(new TestWorkflow()));

        String expectedArn = "arn:aws:states:" + REGION + ":" + ACCOUNT + ":stateMachine:TestWorkflow";

        DescribeStateMachineRequest describeReq = DescribeStateMachineRequest.builder()
                .stateMachineArn(expectedArn).build();
        EasyMock.expect(sfn.describeStateMachine(describeReq))
                .andThrow(StateMachineDoesNotExistException.builder().message("not there").build());

        AtomicReference<CreateStateMachineRequest> captured = new AtomicReference<>();
        EasyMock.expect(sfn.createStateMachine(EasyMock.<CreateStateMachineRequest>anyObject()))
                .andAnswer(() -> {
                    CreateStateMachineRequest req = (CreateStateMachineRequest)
                            EasyMock.getCurrentArguments()[0];
                    captured.set(req);
                    return CreateStateMachineResponse.builder()
                            .stateMachineArn(expectedArn)
                            .stateMachineVersionArn(expectedArn + ":1")
                            .build();
                });

        mockery.replay();
        fc.registerWorkflows();
        mockery.verify();

        CreateStateMachineRequest req = captured.get();
        Assertions.assertEquals("TestWorkflow", req.name());
        Assertions.assertEquals(ROLE, req.roleArn());
        Assertions.assertEquals(StateMachineType.STANDARD, req.type());
        Assertions.assertTrue(req.publish());
        // The definition is the JSON form of the generated ASL.
        Assertions.assertEquals(AslGenerator.generateJson(new TestWorkflow(), REGION, ACCOUNT), req.definition());
    }

    @Test
    public void updatesStateMachineWhenDefinitionDiffers() {
        fc.populateMaps(List.of(new TestWorkflow()));
        String expectedArn = "arn:aws:states:" + REGION + ":" + ACCOUNT + ":stateMachine:TestWorkflow";

        DescribeStateMachineRequest describeReq = DescribeStateMachineRequest.builder()
                .stateMachineArn(expectedArn).build();
        // Return a stale definition; the registrar should call UpdateStateMachine.
        EasyMock.expect(sfn.describeStateMachine(describeReq))
                .andReturn(DescribeStateMachineResponse.builder()
                                   .stateMachineArn(expectedArn)
                                   .name("TestWorkflow")
                                   .roleArn(ROLE)
                                   .definition("{\"StartAt\":\"OldState\",\"States\":{\"OldState\":{\"Type\":\"Succeed\"}}}")
                                   .build());

        AtomicReference<UpdateStateMachineRequest> captured = new AtomicReference<>();
        EasyMock.expect(sfn.updateStateMachine(EasyMock.<UpdateStateMachineRequest>anyObject()))
                .andAnswer(() -> {
                    captured.set((UpdateStateMachineRequest) EasyMock.getCurrentArguments()[0]);
                    return UpdateStateMachineResponse.builder()
                            .stateMachineVersionArn(expectedArn + ":2")
                            .build();
                });

        mockery.replay();
        fc.registerWorkflows();
        mockery.verify();

        UpdateStateMachineRequest req = captured.get();
        Assertions.assertEquals(expectedArn, req.stateMachineArn());
        Assertions.assertEquals(ROLE, req.roleArn());
        Assertions.assertTrue(req.publish());
        Assertions.assertEquals(AslGenerator.generateJson(new TestWorkflow(), REGION, ACCOUNT), req.definition());
    }

    @Test
    public void noOpWhenDefinitionMatches() {
        fc.populateMaps(List.of(new TestWorkflow()));
        String expectedArn = "arn:aws:states:" + REGION + ":" + ACCOUNT + ":stateMachine:TestWorkflow";

        DescribeStateMachineRequest describeReq = DescribeStateMachineRequest.builder()
                .stateMachineArn(expectedArn).build();
        String currentDefinition = AslGenerator.generateJson(new TestWorkflow(), REGION, ACCOUNT);
        EasyMock.expect(sfn.describeStateMachine(describeReq))
                .andReturn(DescribeStateMachineResponse.builder()
                                   .stateMachineArn(expectedArn)
                                   .name("TestWorkflow")
                                   .roleArn(ROLE)
                                   .definition(currentDefinition)
                                   .build());
        // No update or create should be issued.

        mockery.replay();
        fc.registerWorkflows();
        mockery.verify();
    }

    @Test
    public void doesNotUpdateStateMachineWhenUpdatesDisabledAndDefinitionDiffers() {
        config.setUpdateExistingStateMachines(false);
        fc.populateMaps(List.of(new TestWorkflow()));
        String expectedArn = "arn:aws:states:" + REGION + ":" + ACCOUNT + ":stateMachine:TestWorkflow";

        DescribeStateMachineRequest describeReq = DescribeStateMachineRequest.builder()
                .stateMachineArn(expectedArn).build();
        EasyMock.expect(sfn.describeStateMachine(describeReq))
                .andReturn(DescribeStateMachineResponse.builder()
                                   .stateMachineArn(expectedArn)
                                   .name("TestWorkflow")
                                   .roleArn(ROLE)
                                   .definition("{\"StartAt\":\"OldState\",\"States\":{\"OldState\":{\"Type\":\"Succeed\"}}}")
                                   .build());
        // No UpdateStateMachine call expected. EasyMock strict mode fails the test if one is issued.

        mockery.replay();
        fc.registerWorkflows();
        mockery.verify();
    }

    @Test
    public void createsMissingStateMachineEvenWhenUpdatesDisabled() {
        // updateExistingStateMachines only governs the differs-from-deployed path; if the state machine
        // doesn't exist at all, Flux should still create it so workers have something to poll against.
        config.setUpdateExistingStateMachines(false);
        fc.populateMaps(List.of(new TestWorkflow()));
        String expectedArn = "arn:aws:states:" + REGION + ":" + ACCOUNT + ":stateMachine:TestWorkflow";

        DescribeStateMachineRequest describeReq = DescribeStateMachineRequest.builder()
                .stateMachineArn(expectedArn).build();
        EasyMock.expect(sfn.describeStateMachine(describeReq))
                .andThrow(StateMachineDoesNotExistException.builder().message("not there").build());

        AtomicReference<CreateStateMachineRequest> captured = new AtomicReference<>();
        EasyMock.expect(sfn.createStateMachine(EasyMock.<CreateStateMachineRequest>anyObject()))
                .andAnswer(() -> {
                    captured.set((CreateStateMachineRequest) EasyMock.getCurrentArguments()[0]);
                    return CreateStateMachineResponse.builder()
                            .stateMachineArn(expectedArn)
                            .stateMachineVersionArn(expectedArn + ":1")
                            .build();
                });

        mockery.replay();
        fc.registerWorkflows();
        mockery.verify();

        Assertions.assertEquals("TestWorkflow", captured.get().name());
        Assertions.assertEquals(AslGenerator.generateJson(new TestWorkflow(), REGION, ACCOUNT),
                                captured.get().definition());
    }

    @Test
    public void requiresStateMachineRoleArn() {
        fc.populateMaps(List.of(new TestWorkflow()));
        config.setStateMachineRoleArn(""); // intentionally blank to trip the check.
        // setStateMachineRoleArn won't accept null, but blank is allowed and should still fail.
        // Reset to empty string via the field directly: emulate by recreating fc with new config.
        FluxCapacitorConfig blankConfig = new FluxCapacitorConfig();
        blankConfig.setAwsRegion(REGION);
        blankConfig.setAwsAccountId(ACCOUNT);
        FluxCapacitorImpl noRoleFc = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), sfn,
                                                           blankConfig, Clock.systemUTC());
        noRoleFc.populateMaps(List.of(new TestWorkflow()));

        Assertions.assertThrows(FluxException.class, noRoleFc::registerWorkflows);
    }

    @Test
    public void initializeSkipsRegistrationWhenConfigured() throws InterruptedException {
        FluxCapacitorConfig externalConfig = new FluxCapacitorConfig();
        externalConfig.setAwsRegion(REGION);
        externalConfig.setAwsAccountId(ACCOUNT);
        externalConfig.setRegisterWorkflowsOnStartup(false);
        // No stateMachineRoleArn is set — that's intentional, the runtime shouldn't need one.

        FluxCapacitorImpl externalFc = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), sfn,
                                                             externalConfig, Clock.systemUTC());

        // Pollers will start making GetActivityTask calls as soon as initialize() returns; ignore them.
        EasyMock.expect(sfn.getActivityTask(EasyMock.<GetActivityTaskRequest>anyObject()))
                .andReturn(GetActivityTaskResponse.builder().build())
                .anyTimes();
        // The test passes iff no CreateActivity / DescribeStateMachine / CreateStateMachine /
        // UpdateStateMachine call is issued: mockery.verify() at the end of replay() asserts only
        // the recorded expectations were called. EasyMock strict-mode would fail with "Unexpected
        // method call" if any other SfnClient method fires.

        mockery.replay();
        externalFc.initialize(List.of(new TestWorkflow()));
        externalFc.shutdown();
        externalFc.awaitTermination(5, TimeUnit.SECONDS);
        mockery.verify();
    }

    @Test
    public void definitionMatchesIgnoresJsonWhitespace() {
        String a = "{\"StartAt\":\"X\",\"States\":{\"X\":{\"Type\":\"Succeed\"}}}";
        String b = "{\n  \"StartAt\" : \"X\",\n  \"States\" : {\n    \"X\" : { \"Type\" : \"Succeed\" }\n  }\n}";
        Assertions.assertTrue(FluxCapacitorImpl.definitionMatches(a, b));
    }

    @Test
    public void definitionMatchesReturnsFalseForNullExisting() {
        Assertions.assertFalse(FluxCapacitorImpl.definitionMatches(null, "{}"));
    }
}
