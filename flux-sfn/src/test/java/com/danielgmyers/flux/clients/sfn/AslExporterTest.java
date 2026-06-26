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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.PartitionIdGenerator;
import com.danielgmyers.flux.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AslExporterTest {

    private static final String REGION = "us-east-1";
    private static final String ACCOUNT = "123456789012";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static class StepOne implements WorkflowStep {
        @StepApply public void apply() {}
    }

    public static class StepTwo implements WorkflowStep {
        @StepApply public void apply() {}
    }

    public static class PartStep implements PartitionedWorkflowStep {
        @PartitionIdGenerator
        public PartitionIdGeneratorResult gen() {
            return PartitionIdGeneratorResult.create(Set.of("a", "b"));
        }

        @StepApply
        public void apply(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {}
    }

    public static class WorkflowA implements Workflow {
        private final WorkflowGraph graph;
        public WorkflowA() {
            StepOne one = new StepOne();
            StepTwo two = new StepTwo();
            this.graph = new WorkflowGraphBuilder(one)
                    .alwaysTransition(one, two)
                    .addStep(two).alwaysClose(two)
                    .build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    public static class WorkflowB implements Workflow {
        private final WorkflowGraph graph;
        public WorkflowB() {
            PartStep p = new PartStep();
            this.graph = new WorkflowGraphBuilder(p).alwaysClose(p).build();
        }
        @Override public WorkflowGraph getGraph() { return graph; }
    }

    @Test
    public void writeAslDefinitionEmitsJsonataDefinitionToTheStream() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AslExporter.writeAslDefinition(new WorkflowA(), REGION, ACCOUNT, out);

        JsonNode tree = MAPPER.readTree(out.toString(StandardCharsets.UTF_8));
        Assertions.assertEquals("JSONata", tree.get("QueryLanguage").asText());
        Assertions.assertEquals("StepOne", tree.get("StartAt").asText());
    }

    @Test
    public void aslDefinitionStringHelperMatchesStreamOutput() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AslExporter.writeAslDefinition(new WorkflowA(), REGION, ACCOUNT, out);
        String fromStream = out.toString(StandardCharsets.UTF_8);
        String fromHelper = AslExporter.aslDefinition(new WorkflowA(), REGION, ACCOUNT);
        Assertions.assertEquals(fromStream, fromHelper);
    }

    @Test
    public void writeAslDefinitionDoesNotCloseTheCallerOwnedStream() throws Exception {
        class CloseTrackingStream extends OutputStream {
            boolean closed = false;
            final ByteArrayOutputStream delegate = new ByteArrayOutputStream();
            @Override public void write(int b) { delegate.write(b); }
            @Override public void close() { closed = true; }
        }
        CloseTrackingStream out = new CloseTrackingStream();
        AslExporter.writeAslDefinition(new WorkflowA(), REGION, ACCOUNT, out);
        Assertions.assertFalse(out.closed, "Caller owns the stream — exporter must not close it");
        Assertions.assertTrue(out.delegate.size() > 0);
    }

    @Test
    public void stateMachineNameMatchesWorkflowClassName() {
        Assertions.assertEquals("WorkflowA", AslExporter.stateMachineName(new WorkflowA()));
        Assertions.assertEquals("WorkflowB", AslExporter.stateMachineName(new WorkflowB()));
    }

    @Test
    public void listExpectedActivityNamesIncludesGeneratorForPartitionedSteps() {
        List<String> names = AslExporter.listExpectedActivityNames(new WorkflowB());
        Assertions.assertTrue(names.contains("WorkflowB-PartStep"));
        Assertions.assertTrue(names.contains("WorkflowB-PartStep_gen"),
                              "Partitioned steps should also need the generator activity");
    }

    @Test
    public void listExpectedActivityNamesForRegularWorkflowExcludesGeneratorSuffix() {
        List<String> names = AslExporter.listExpectedActivityNames(new WorkflowA());
        Assertions.assertTrue(names.contains("WorkflowA-StepOne"));
        Assertions.assertTrue(names.contains("WorkflowA-StepTwo"));
        Assertions.assertTrue(names.stream().noneMatch(n -> n.endsWith("_gen")));
    }

    @Test
    public void rejectsNullWorkflow() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> AslExporter.writeAslDefinition(null, REGION, ACCOUNT, new ByteArrayOutputStream()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> AslExporter.aslDefinition(null, REGION, ACCOUNT));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> AslExporter.listExpectedActivityNames(null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> AslExporter.stateMachineName(null));
    }

    @Test
    public void rejectsNullStream() throws IOException {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> AslExporter.writeAslDefinition(new WorkflowA(), REGION, ACCOUNT, null));
    }
}
