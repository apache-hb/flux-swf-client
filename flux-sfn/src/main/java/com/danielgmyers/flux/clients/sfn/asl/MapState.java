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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * ASL "Map" state — iterates over an array (or items computed from the input) and runs an inline
 * sub-state-machine ({@code ItemProcessor}) for each item.
 *
 * <p>Flux uses Map states to implement partitioned workflow steps: the partition-id generator activity
 * returns the list of partition IDs, the Map state iterates them, and each iteration invokes the
 * partition activity with the partition ID and the merged workflow attributes.</p>
 *
 * <p>Pass either {@code next} (and {@code end == false}) or {@code end == true} — never both. {@code items},
 * {@code itemSelector}, and {@code output} accept either a JSON value or a JSONata expression string like
 * {@code "{% $states.input %}"}.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"Type", "Comment", "Items", "ItemSelector", "ItemProcessor",
                    "Output", "Retry", "Catch", "Next", "End"})
public final class MapState extends AslState {

    @JsonProperty("Items")
    private final Object items;

    @JsonProperty("ItemSelector")
    private final Object itemSelector;

    @JsonProperty("ItemProcessor")
    private final ItemProcessor itemProcessor;

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

    public MapState(String comment, Object items, Object itemSelector, ItemProcessor itemProcessor, Object output,
                    List<RetryRule> retry, List<CatchRule> catchRules, String next, boolean end) {
        super("Map", comment);
        if (itemProcessor == null) {
            throw new IllegalArgumentException("Map state requires an ItemProcessor");
        }
        if ((next == null) == !end) {
            throw new IllegalArgumentException("Map state requires exactly one of Next or End=true");
        }
        this.items = items;
        this.itemSelector = itemSelector;
        this.itemProcessor = itemProcessor;
        this.output = output;
        this.retry = retry == null ? null : Collections.unmodifiableList(new ArrayList<>(retry));
        this.catchRules = catchRules == null ? null : Collections.unmodifiableList(new ArrayList<>(catchRules));
        this.next = next;
        this.end = end ? Boolean.TRUE : null;
    }

    public Object getItems() {
        return items;
    }

    public ItemProcessor getItemProcessor() {
        return itemProcessor;
    }

    public String getNext() {
        return next;
    }

    public Boolean getEnd() {
        return end;
    }

    /** Nested ASL "ItemProcessor" — an inline sub-state-machine run for each item. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"ProcessorConfig", "StartAt", "States"})
    public static final class ItemProcessor {
        @JsonProperty("ProcessorConfig")
        private final Map<String, Object> processorConfig;

        @JsonProperty("StartAt")
        private final String startAt;

        @JsonProperty("States")
        private final Map<String, AslState> states;

        public ItemProcessor(String startAt, Map<String, AslState> states) {
            if (startAt == null || startAt.isEmpty()) {
                throw new IllegalArgumentException("ItemProcessor.StartAt must not be empty");
            }
            if (states == null || states.isEmpty()) {
                throw new IllegalArgumentException("ItemProcessor.States must not be empty");
            }
            this.startAt = startAt;
            this.states = Collections.unmodifiableMap(new LinkedHashMap<>(states));
            this.processorConfig = Map.of("Mode", "INLINE");
        }

        public String getStartAt() {
            return startAt;
        }

        public Map<String, AslState> getStates() {
            return states;
        }
    }
}
