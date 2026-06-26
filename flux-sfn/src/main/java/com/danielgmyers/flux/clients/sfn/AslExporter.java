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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.danielgmyers.flux.clients.sfn.asl.AslGenerator;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraphNode;

/**
 * Build-time helpers for producing the ASL JSON the runtime would otherwise install via
 * {@code CreateStateMachine}. Useful when state machines are managed externally (CDK / Terraform /
 * raw CloudFormation) and the Flux runtime should run only the worker side.
 *
 * <p>This class makes no AWS SDK calls and has no runtime dependencies on FluxCapacitor — it can be
 * invoked from a build step or a small main without standing up the worker pool.</p>
 *
 * <p>Callers control where output goes. A typical CDK / build-tool integration writes one workflow
 * at a time to a chosen {@link OutputStream}:</p>
 * <pre>
 *   try (var out = Files.newOutputStream(targetDir.resolve(myWorkflow.getClass().getSimpleName() + ".asl.json"))) {
 *       AslExporter.writeAslDefinition(myWorkflow, awsRegion, awsAccountId, out);
 *   }
 * </pre>
 */
public final class AslExporter {

    private AslExporter() {}

    /**
     * Returns the SFN state-machine name the runtime expects for the given workflow (i.e., its simple
     * class name). Callers managing state machines externally should use this name verbatim when
     * registering the state machine.
     */
    public static String stateMachineName(Workflow workflow) {
        if (workflow == null) {
            throw new IllegalArgumentException("workflow must not be null");
        }
        return TaskNaming.workflowName(workflow);
    }

    /**
     * Generates the ASL JSON definition for a single workflow and writes it to the provided stream.
     * The stream is <em>not</em> closed by this method — the caller owns it.
     *
     * @param workflow     the Flux workflow to convert
     * @param awsRegion    AWS region where the activities live (used to build activity ARNs in the ASL)
     * @param awsAccountId AWS account ID where the activities live (used to build activity ARNs in the ASL)
     * @param out          stream the ASL JSON will be written to (UTF-8)
     */
    public static void writeAslDefinition(Workflow workflow, String awsRegion, String awsAccountId,
                                          OutputStream out) throws IOException {
        if (workflow == null) {
            throw new IllegalArgumentException("workflow must not be null");
        }
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        String json = AslGenerator.generatePrettyJson(workflow, awsRegion, awsAccountId);
        out.write(json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Convenience: returns the ASL JSON definition as a string. Equivalent to capturing the output of
     * {@link #writeAslDefinition} into a {@code ByteArrayOutputStream}.
     */
    public static String aslDefinition(Workflow workflow, String awsRegion, String awsAccountId) {
        if (workflow == null) {
            throw new IllegalArgumentException("workflow must not be null");
        }
        return AslGenerator.generatePrettyJson(workflow, awsRegion, awsAccountId);
    }

    /**
     * Returns the activity names the runtime expects to find registered for the given workflow. Each
     * regular step contributes one name (the form {@code <WorkflowName>-<StepName>}); each partitioned
     * step additionally contributes the generator-activity name (the same name with the
     * {@code _gen} suffix).
     */
    public static List<String> listExpectedActivityNames(Workflow workflow) {
        if (workflow == null) {
            throw new IllegalArgumentException("workflow must not be null");
        }
        List<String> names = new ArrayList<>();
        String workflowName = TaskNaming.workflowName(workflow);
        for (Map.Entry<Class<? extends WorkflowStep>, WorkflowGraphNode> e
                : workflow.getGraph().getNodes().entrySet()) {
            String activityName = workflowName + "-" + e.getKey().getSimpleName();
            names.add(activityName);
            if (PartitionedWorkflowStep.class.isAssignableFrom(e.getKey())) {
                names.add(activityName + SfnArnFormatter.PARTITION_GENERATOR_ACTIVITY_SUFFIX);
            }
        }
        return names;
    }
}
