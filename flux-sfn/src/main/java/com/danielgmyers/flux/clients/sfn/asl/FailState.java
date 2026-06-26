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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ASL terminal "Fail" state.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class FailState extends AslState {

    @JsonProperty("Error")
    private final String error;

    @JsonProperty("Cause")
    private final String cause;

    public FailState(String error, String cause, String comment) {
        super("Fail", comment);
        this.error = error;
        this.cause = cause;
    }

    public String getError() {
        return error;
    }

    public String getCause() {
        return cause;
    }
}
