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
 * ASL "Choice" state — routes to one of several next states. In JSONata mode each {@link Choice}
 * carries a {@code Condition} (a JSONata expression evaluating to a boolean).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"Type", "Comment", "Choices", "Default"})
public final class ChoiceState extends AslState {

    @JsonProperty("Choices")
    private final List<Choice> choices;

    @JsonProperty("Default")
    private final String defaultNext;

    public ChoiceState(String comment, List<Choice> choices, String defaultNext) {
        super("Choice", comment);
        if (choices == null || choices.isEmpty()) {
            throw new IllegalArgumentException("Choice state requires at least one Choices entry");
        }
        this.choices = Collections.unmodifiableList(new ArrayList<>(choices));
        this.defaultNext = defaultNext;
    }

    public List<Choice> getChoices() {
        return choices;
    }

    public String getDefaultNext() {
        return defaultNext;
    }

    /**
     * A single Choice branch consisting of a JSONata {@code Condition} expression and the name of the
     * state to route to if the condition evaluates to true.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"Condition", "Next"})
    public static final class Choice {
        @JsonProperty("Condition")
        private final String condition;

        @JsonProperty("Next")
        private final String next;

        public Choice(String condition, String next) {
            if (condition == null || condition.isEmpty()) {
                throw new IllegalArgumentException("Choice Condition is required");
            }
            if (next == null || next.isEmpty()) {
                throw new IllegalArgumentException("Choice Next is required");
            }
            this.condition = condition;
            this.next = next;
        }

        public String getCondition() {
            return condition;
        }

        public String getNext() {
            return next;
        }
    }
}
