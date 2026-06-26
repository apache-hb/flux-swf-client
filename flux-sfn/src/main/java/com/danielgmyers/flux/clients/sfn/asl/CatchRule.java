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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * An ASL "Catch" rule on a Task / Map state. Used to route uncaught errors to a terminal state.
 *
 * In JSONata mode the error detail is available via {@code $states.errorOutput} in the next state's
 * fields; we don't need to attach it to state input ourselves.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"ErrorEquals", "Next"})
public final class CatchRule {

    @JsonProperty("ErrorEquals")
    private final List<String> errorEquals;

    @JsonProperty("Next")
    private final String next;

    public CatchRule(List<String> errorEquals, String next) {
        if (errorEquals == null || errorEquals.isEmpty()) {
            throw new IllegalArgumentException("CatchRule requires at least one ErrorEquals entry");
        }
        if (next == null || next.isEmpty()) {
            throw new IllegalArgumentException("CatchRule requires a Next state name");
        }
        this.errorEquals = List.copyOf(errorEquals);
        this.next = next;
    }

    public List<String> getErrorEquals() {
        return errorEquals;
    }

    public String getNext() {
        return next;
    }
}
