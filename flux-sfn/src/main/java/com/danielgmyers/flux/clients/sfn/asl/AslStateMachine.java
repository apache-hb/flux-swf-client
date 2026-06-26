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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Top-level Amazon States Language state machine definition.
 *
 * Serialize to ASL JSON with {@link #toJson()} (compact) or {@link #toPrettyJson()}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"Comment", "StartAt", "Version", "QueryLanguage", "TimeoutSeconds", "States"})
public final class AslStateMachine {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectMapper PRETTY_MAPPER
            = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    @JsonProperty("Comment")
    private final String comment;

    @JsonProperty("StartAt")
    private final String startAt;

    @JsonProperty("Version")
    private final String version;

    @JsonProperty("QueryLanguage")
    private final String queryLanguage;

    @JsonProperty("TimeoutSeconds")
    private final Long timeoutSeconds;

    @JsonProperty("States")
    private final Map<String, AslState> states;

    public AslStateMachine(String comment, String startAt, String queryLanguage, Long timeoutSeconds,
                           Map<String, AslState> states) {
        if (startAt == null || startAt.isEmpty()) {
            throw new IllegalArgumentException("startAt must not be empty");
        }
        if (states == null || states.isEmpty()) {
            throw new IllegalArgumentException("states must not be empty");
        }
        if (!states.containsKey(startAt)) {
            throw new IllegalArgumentException("startAt state '" + startAt + "' is not defined in states");
        }
        this.comment = comment;
        this.startAt = startAt;
        this.version = "1.0";
        this.queryLanguage = queryLanguage;
        this.timeoutSeconds = timeoutSeconds;
        // Preserve insertion order so generated ASL is deterministic.
        this.states = Collections.unmodifiableMap(new LinkedHashMap<>(states));
    }

    public String getStartAt() {
        return startAt;
    }

    public Long getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public Map<String, AslState> getStates() {
        return states;
    }

    /**
     * Serializes this state machine to a compact ASL JSON string.
     */
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize ASL state machine", e);
        }
    }

    /**
     * Serializes this state machine to a pretty-printed ASL JSON string.
     */
    public String toPrettyJson() {
        try {
            return PRETTY_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize ASL state machine", e);
        }
    }
}
