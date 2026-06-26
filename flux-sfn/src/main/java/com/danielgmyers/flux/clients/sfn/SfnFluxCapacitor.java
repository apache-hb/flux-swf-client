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
import java.util.List;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.wf.Workflow;

/**
 * The SFN-specific {@link FluxCapacitor} interface. Adds operations that are particular to the Step
 * Functions backend, most notably the ability to emit the ASL JSON the runtime would otherwise install
 * itself — which lets a separate process (e.g. a CDK app) create the state machines at deploy time and
 * have the Flux runtime simply run the workers.
 *
 * <p>When using externally-managed state machines:</p>
 * <ol>
 *   <li>At build time, call {@link #writeAslDefinition(Workflow, String, String, OutputStream)} for
 *       each workflow you intend to run, persisting the output for consumption by your CDK / Terraform
 *       / CloudFormation app.</li>
 *   <li>Deploy state machines named {@code <WorkflowClassName>} (no other naming scheme is recognized).
 *       Activities should be named {@code <WorkflowClassName>-<StepClassName>}; partitioned steps
 *       additionally need {@code <WorkflowClassName>-<StepClassName>_gen} for the partition-id
 *       generator activity.</li>
 *   <li>At runtime, set {@link FluxCapacitorConfig#setRegisterWorkflowsOnStartup(boolean)} to
 *       {@code false} so the runtime doesn't try to create or update those resources.</li>
 * </ol>
 */
public interface SfnFluxCapacitor extends FluxCapacitor {

    /**
     * Writes the ASL JSON definition for {@code workflow} to {@code out}. The stream is not closed by
     * this method — the caller owns it.
     *
     * @param workflow     the Flux workflow to convert
     * @param awsRegion    AWS region where the activities live (used to build activity ARNs in the ASL)
     * @param awsAccountId AWS account ID where the activities live (used to build activity ARNs in the ASL)
     * @param out          stream the ASL JSON will be written to (UTF-8)
     */
    void writeAslDefinition(Workflow workflow, String awsRegion, String awsAccountId, OutputStream out) throws IOException;

    /**
     * Returns the activity names the SFN backend expects to exist for {@code workflow} — useful when
     * generating the CDK plumbing to register the activities alongside the state machine.
     */
    List<String> listExpectedActivityNames(Workflow workflow);
}
