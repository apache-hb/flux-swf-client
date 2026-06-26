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

package com.danielgmyers.flux.clients.sfn.asl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * ASL "Task" state — invokes the activity at the given Resource ARN.
 *
 * In JSONata mode, the activity input is constructed via {@code Arguments} (which may reference
 * {@code $states.context} for built-in attributes sourced from Step Functions itself) and the state
 * output is controlled via {@code Output}.
 *
 * <p>Pass either {@code next} (and {@code end == false}) or {@code end == true} — never both. {@code arguments}
 * and {@code output} accept either a JSON value or a JSONata expression string like
 * {@code "{% $states.input %}"}.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"Type", "Comment", "Resource", "HeartbeatSeconds",
                    "Arguments", "Output", "Retry", "Catch", "Next", "End"})
public final class TaskState extends AslState {

    @JsonProperty("Resource")
    private final String resource;

    @JsonProperty("HeartbeatSeconds")
    private final Long heartbeatSeconds;

    @JsonProperty("Arguments")
    private final Object arguments;

    @JsonProperty("Output")
    private final Object output;

    @JsonProperty("Retry")
    private final List<RetryRule> retry;

    @JsonProperty("Catch")
    private final List<CatchRule> catchRules;

    @JsonProperty("Next")
    private final String next;

    @JsonProperty("End")
    private final Boolean end;

    public TaskState(String comment, String resource, Long heartbeatSeconds, Object arguments, Object output,
                     List<RetryRule> retry, List<CatchRule> catchRules, String next, boolean end) {
        super("Task", comment);
        if (resource == null || resource.isEmpty()) {
            throw new IllegalArgumentException("Task Resource is required");
        }
        if ((next == null) == !end) {
            throw new IllegalArgumentException("Task state requires exactly one of Next or End=true");
        }
        this.resource = resource;
        this.heartbeatSeconds = heartbeatSeconds;
        this.arguments = arguments;
        this.output = output;
        this.retry = retry == null ? null : Collections.unmodifiableList(new ArrayList<>(retry));
        this.catchRules = catchRules == null ? null : Collections.unmodifiableList(new ArrayList<>(catchRules));
        this.next = next;
        this.end = end ? Boolean.TRUE : null;
    }

    public String getResource() {
        return resource;
    }

    public Object getOutput() {
        return output;
    }

    public List<RetryRule> getRetry() {
        return retry;
    }

    public List<CatchRule> getCatchRules() {
        return catchRules;
    }

    public String getNext() {
        return next;
    }

    public Boolean getEnd() {
        return end;
    }
}
