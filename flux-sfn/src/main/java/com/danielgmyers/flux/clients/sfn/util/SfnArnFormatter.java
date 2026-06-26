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

package com.danielgmyers.flux.clients.sfn.util;

import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;

public final class SfnArnFormatter {

    /**
     * Suffix appended to the activity name of a partitioned step's partition-id generator activity.
     * The generator activity is a separate SFN activity that runs the {@code @PartitionIdGenerator}
     * method and returns the list of partition IDs for the partitioned step to iterate over.
     */
    public static final String PARTITION_GENERATOR_ACTIVITY_SUFFIX = "_gen";

    private SfnArnFormatter() {}

    // We don't use the SDK's Arn class to generate these because it only works for generating ARNs that have a qualifier,
    // but only some resources use the qualifier.
    private static String sfnArn(String region, String accountId, String resourceType, String resourceId) {
        return String.format("arn:aws:states:%s:%s:%s:%s", region, accountId, resourceType, resourceId);
    }

    public static String workflowArn(String region, String accountId, Class<? extends Workflow> workflowClass) {
        return sfnArn(region, accountId, "stateMachine", workflowClass.getSimpleName());
    }

    public static String executionArn(String region, String accountId, Class<? extends Workflow> workflowClass,
                                      String executionId) {
        String resourceIdWithQualifier = String.format("%s:%s", workflowClass.getSimpleName(), executionId);
        return sfnArn(region, accountId, "execution", resourceIdWithQualifier);
    }

    public static String activityArn(String region, String accountId, Class<? extends Workflow> workflowClass,
                                     Class<? extends WorkflowStep> stepClass) {
        String resourceId = String.format("%s-%s", workflowClass.getSimpleName(), stepClass.getSimpleName());
        return sfnArn(region, accountId, "activity", resourceId);
    }

    /**
     * Returns the ARN of the partition-id generator activity for a partitioned step.
     */
    public static String partitionGeneratorActivityArn(String region, String accountId,
                                                       Class<? extends Workflow> workflowClass,
                                                       Class<? extends WorkflowStep> stepClass) {
        String resourceId = String.format("%s-%s%s", workflowClass.getSimpleName(), stepClass.getSimpleName(),
                                          PARTITION_GENERATOR_ACTIVITY_SUFFIX);
        return sfnArn(region, accountId, "activity", resourceId);
    }
}
