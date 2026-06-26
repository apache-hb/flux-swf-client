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

import com.danielgmyers.flux.wf.Workflow;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;

/**
 * Utility class for storing/accessing configuration data provided to the SFN integration tests.
 *
 * <p>Required system properties:</p>
 * <ul>
 *   <li>{@code awsAccountId} — the AWS account ID Flux should target.</li>
 *   <li>{@code stateMachineRoleArn} — IAM role ARN that Step Functions will assume when running the
 *       registered state machines. The role must be able to invoke the activities Flux creates and
 *       allow {@code states.amazonaws.com} as a trusted principal.</li>
 * </ul>
 *
 * <p>Optional system properties (with defaults):</p>
 * <ul>
 *   <li>{@code awsRegion} — defaults to {@code us-east-1}.</li>
 *   <li>{@code remoteRegion} — defaults to {@code us-east-1} (only used by the remote-executor test).</li>
 *   <li>{@code remoteAwsAccountId} — defaults to the local {@code awsAccountId}.</li>
 *   <li>{@code sfnEndpoint} — optional custom SFN endpoint URL.</li>
 * </ul>
 */
public final class TestConfig {

    private TestConfig() {}

    private static volatile String cachedAccountId;

    public static String getAwsRegion() {
        return System.getProperty("awsRegion", "us-east-1");
    }

    /**
     * Returns the AWS account ID to use, taken from the {@code awsAccountId} system property if set,
     * otherwise auto-discovered via STS using the default credentials provider.
     */
    public static String getAwsAccountId() {
        String fromProp = System.getProperty("awsAccountId");
        if (fromProp != null && !fromProp.isEmpty()) {
            return fromProp;
        }
        if (cachedAccountId == null) {
            synchronized (TestConfig.class) {
                if (cachedAccountId == null) {
                    try (StsClient sts = StsClient.builder()
                            .credentialsProvider(DefaultCredentialsProvider.create())
                            .region(Region.of(getAwsRegion()))
                            .build()) {
                        cachedAccountId = sts.getCallerIdentity().account();
                    }
                }
            }
        }
        return cachedAccountId;
    }

    public static String getStateMachineRoleArn() {
        String fromProp = System.getProperty("stateMachineRoleArn");
        if (fromProp != null && !fromProp.isEmpty()) {
            return fromProp;
        }
        // Default to a conventional role name in the discovered account.
        return "arn:aws:iam::" + getAwsAccountId() + ":role/FluxSfnIntegrationTestRole";
    }

    public static String getSfnEndpoint() {
        return System.getProperty("sfnEndpoint", null);
    }

    public static RemoteSfnClientConfig getRemoteSfnClientConfig() {
        RemoteSfnClientConfig config = new RemoteSfnClientConfig();
        config.setAwsRegion(System.getProperty("remoteRegion", "us-east-1"));
        config.setAwsAccountId(System.getProperty("remoteAwsAccountId", getAwsAccountId()));
        if (System.getProperty("remoteSfnEndpoint") != null) {
            config.setSfnEndpoint(System.getProperty("remoteSfnEndpoint"));
        }
        return config;
    }

    /**
     * Generates a Flux configuration with small worker pools to minimize throttling risk when test suites
     * run concurrently.
     */
    public static FluxCapacitorConfig generateFluxConfig(int workerPoolSize) {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setAwsRegion(getAwsRegion());
        config.setAwsAccountId(getAwsAccountId());
        config.setStateMachineRoleArn(getStateMachineRoleArn());
        if (getSfnEndpoint() != null) {
            config.setSfnEndpoint(getSfnEndpoint());
        }

        TaskListConfig tasklistConfig = new TaskListConfig();
        tasklistConfig.setActivityTaskThreadCount(workerPoolSize);
        // 1 poller per activity is sufficient: now that the poller doesn't hold a worker permit
        // during the GetActivityTask long-poll, that single poll is always in flight and gets
        // delivered tasks immediately. Higher counts only multiply HTTP connection pressure.
        tasklistConfig.setActivityPollerThreadCount(1);

        config.putTaskListConfig(Workflow.DEFAULT_TASK_LIST_NAME, tasklistConfig);
        return config;
    }

}
