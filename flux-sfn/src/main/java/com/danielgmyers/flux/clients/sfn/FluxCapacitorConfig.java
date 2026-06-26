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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

/**
 * Container for configuration data used by Flux at runtime.
 */
public class FluxCapacitorConfig {

    private String awsRegion;
    private String awsAccountId;
    private String stateMachineRoleArn;
    private boolean registerWorkflowsOnStartup = true;
    private boolean updateExistingStateMachines = true;
    private Double exponentialBackoffBase;
    private Function<String, String> hostnameTransformerForPollerIdentity = Function.identity();
    private String sfnEndpoint;
    private ClientOverrideConfiguration clientOverrideConfiguration;
    private final Map<String, TaskListConfig> taskListConfigs = new HashMap<>();
    private Function<String, RemoteSfnClientConfig> remoteSfnClientConfigProvider;

    public String getAwsRegion() {
        return awsRegion;
    }

    /**
     * Configures the AWS region that Flux uses to communicate with Step Functions.
     *
     * This value is required.
     */
    public void setAwsRegion(String awsRegion) {
        if (awsRegion == null) {
            throw new IllegalArgumentException("awsRegion may not be null.");
        }
        this.awsRegion = awsRegion;
    }

    public String getAwsAccountId() {
        return awsAccountId;
    }

    /**
     * Configures the AWS Account ID that flux should use when constructing resource identifiers for
     * Step Functions resources. This is _assumed_ to be the account matching the credentials
     * provided to Flux for making Step Functions API calls.
     *
     * This value is required.
     */
    public void setAwsAccountId(String awsAccountId) {
        if (awsAccountId == null) {
            throw new IllegalArgumentException("awsAccountId may not be null.");
        }
        this.awsAccountId = awsAccountId;
    }

    public String getStateMachineRoleArn() {
        return stateMachineRoleArn;
    }

    /**
     * Configures the IAM role ARN that Step Functions will assume when running registered workflows.
     * This role must be able to invoke the activities registered by Flux.
     *
     * This value is required if Flux is going to register workflows on startup. If you only plan to
     * use Flux to start executions of workflows registered out-of-band, you may leave this unset.
     */
    public void setStateMachineRoleArn(String stateMachineRoleArn) {
        if (stateMachineRoleArn == null) {
            throw new IllegalArgumentException("stateMachineRoleArn may not be null.");
        }
        this.stateMachineRoleArn = stateMachineRoleArn;
    }

    public boolean isRegisterWorkflowsOnStartup() {
        return registerWorkflowsOnStartup;
    }

    /**
     * Controls whether Flux registers Step Functions activities and state machines on startup.
     *
     * <p>Defaults to {@code true}, which matches Flux's traditional behavior: the runtime owns the state
     * machine and activity definitions.</p>
     *
     * <p>Set to {@code false} when the state machines (and activities) are managed externally — for
     * example, deployed via CDK using the ASL JSON produced by {@link com.danielgmyers.flux.clients.sfn.asl.AslGenerator}
     * at build time. In that mode {@code FluxCapacitor.initialize()} sets up the worker pollers and
     * periodic-workflow scheduler but makes no Step Functions {@code CreateActivity},
     * {@code CreateStateMachine}, or {@code UpdateStateMachine} calls.</p>
     *
     * <p>When this is {@code false}, {@link #setStateMachineRoleArn(String)} is not required.</p>
     */
    public void setRegisterWorkflowsOnStartup(boolean registerWorkflowsOnStartup) {
        this.registerWorkflowsOnStartup = registerWorkflowsOnStartup;
    }

    public boolean isUpdateExistingStateMachines() {
        return updateExistingStateMachines;
    }

    /**
     * Controls whether Flux issues {@code UpdateStateMachine} when an existing state machine's
     * definition does not match the Flux-generated ASL.
     *
     * <p>Defaults to {@code true}: if the existing definition differs, Flux replaces it with a new
     * published version. This is the historic behaviour and is appropriate when Flux owns the state
     * machine.</p>
     *
     * <p>Set to {@code false} when state machines are deployed by an external system (e.g. CDK or
     * a CI/CD pipeline running {@code AslExporter} during build) and Flux should treat the deployed
     * definition as authoritative. With this disabled and a mismatched definition present, Flux
     * still creates the state machine if it is missing, but it will only log a warning when the
     * existing definition differs, leaving the deployed state machine untouched.</p>
     *
     * <p>Has no effect when {@link #setRegisterWorkflowsOnStartup(boolean)} is {@code false}.</p>
     */
    public void setUpdateExistingStateMachines(boolean updateExistingStateMachines) {
        this.updateExistingStateMachines = updateExistingStateMachines;
    }

    public Double getExponentialBackoffBase() {
        return exponentialBackoffBase;
    }

    /**
     * This overrides the base used to calculate the exponential backoff duration, which defaults to 1.25.
     * Even if this value is set, individual steps can still use a different base via the @StepApply annotation.
     *
     * If set, this value must not be less than 1.
     */
    public void setExponentialBackoffBase(Double exponentialBackoffBase) {
        if (exponentialBackoffBase == null) {
            throw new IllegalArgumentException("exponentialBackoffBase may not be null.");
        }
        if (exponentialBackoffBase < 1.0) {
            throw new IllegalArgumentException("exponentialBackoffBase must be at least 1.");
        }
        this.exponentialBackoffBase = exponentialBackoffBase;
    }

    public Function<String, String> getHostnameTransformerForPollerIdentity() {
        return hostnameTransformerForPollerIdentity;
    }

    /**
     * If specified, Flux will use the provided function to transform the hostname prior to using it to poll for work.
     */
    public void setHostnameTransformerForPollerIdentity(Function<String, String> hostnameTransformerForPollerIdentity) {
        if (hostnameTransformerForPollerIdentity == null) {
            throw new IllegalArgumentException("hostnameTransformerForPollerIdentity may not be null.");
        }
        this.hostnameTransformerForPollerIdentity = hostnameTransformerForPollerIdentity;
    }

    public String getSfnEndpoint() {
        return sfnEndpoint;
    }

    /**
     * Sets the endpoint used to communicate with Step Functions.
     *
     * This configuration value is optional; if not specified, the endpoint will be determined automatically
     * using the awsRegion configuration value.
     *
     * Note that the provided endpoint value must begin with a valid URI scheme, which should most likely be "https://".
     */
    public void setSfnEndpoint(String sfnEndpoint) {
        if (sfnEndpoint == null) {
            throw new IllegalArgumentException("sfnEndpoint may not be null.");
        }
        this.sfnEndpoint = sfnEndpoint;
    }

    public ClientOverrideConfiguration getClientOverrideConfiguration() {
        return clientOverrideConfiguration;
    }

    /**
     * Overrides the default configuration used by the SfnClient created by Flux.
     *
     * Note that by default, Flux already overrides the client's default retry policy by disabling it
     * entirely, and implements its own retry and backoff logic, in order to generate clean metrics.
     * However, if this override configuration specifies a retry policy, then Flux will not disable
     * retries, but Flux's Step Functions API metrics will no longer reflect only durations for single API calls.
     */
    public void setClientOverrideConfiguration(ClientOverrideConfiguration clientOverrideConfiguration) {
        this.clientOverrideConfiguration = clientOverrideConfiguration;
    }

    /**
     * Stores the provided task list configuration for the specified task list name.
     *
     * Task lists that are not explicitly configured will get the default task list configuration;
     * see TaskListConfig for more information on the default values.
     */
    public void putTaskListConfig(String taskListName, TaskListConfig config) {
        if (taskListName == null) {
            throw new IllegalArgumentException("taskListName may not be null.");
        }
        if (config == null) {
            throw new IllegalArgumentException("config may not be null.");
        }
        this.taskListConfigs.put(taskListName, config);
    }

    public TaskListConfig getTaskListConfig(String taskList) {
        return taskListConfigs.computeIfAbsent(taskList, name -> new TaskListConfig());
    }

    /**
     * Specifies a callback function that Flux can use to retrieve configuration for a RemoteWorkflowExecutor.
     * The input to this callback is an arbitrary "endpoint id". This string is provided by the user
     * as input to FluxCapacitor.getRemoteWorkflowExecutor().
     * For example, the user might choose to map the endpoint id "standby-region" to a particular remote region config.
     */
    public void setRemoteSfnClientConfigProvider(Function<String, RemoteSfnClientConfig> remoteSfnClientConfigProvider) {
        this.remoteSfnClientConfigProvider = remoteSfnClientConfigProvider;
    }

    public Function<String, RemoteSfnClientConfig> getRemoteSfnClientConfigProvider() {
        return remoteSfnClientConfigProvider;
    }
}
