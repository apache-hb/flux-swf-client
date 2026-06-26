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
 * Base type for all ASL state definitions. The discriminator field is "Type".
 *
 * Subclasses must call the super constructor with their literal ASL type name
 * (e.g. "Task", "Choice", "Succeed", "Fail", "Wait", "Pass").
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class AslState {

    @JsonProperty("Type")
    private final String type;

    @JsonProperty("Comment")
    private final String comment;

    protected AslState(String type, String comment) {
        this.type = type;
        this.comment = comment;
    }

    public String getType() {
        return type;
    }

    public String getComment() {
        return comment;
    }
}
