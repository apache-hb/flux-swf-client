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
 * An ASL "Retry" rule on a Task state.
 *
 * Step Functions exposes IntervalSeconds, MaxAttempts, BackoffRate, MaxDelaySeconds, and JitterStrategy.
 * The set of fields used by Flux mirrors the retry configuration exposed via {@code @StepApply}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"ErrorEquals", "IntervalSeconds", "MaxAttempts", "BackoffRate", "MaxDelaySeconds", "JitterStrategy"})
public final class RetryRule {

    public static final String JITTER_STRATEGY_FULL = "FULL";

    @JsonProperty("ErrorEquals")
    private final List<String> errorEquals;

    @JsonProperty("IntervalSeconds")
    private final Long intervalSeconds;

    @JsonProperty("MaxAttempts")
    private final Long maxAttempts;

    @JsonProperty("BackoffRate")
    private final Double backoffRate;

    @JsonProperty("MaxDelaySeconds")
    private final Long maxDelaySeconds;

    @JsonProperty("JitterStrategy")
    private final String jitterStrategy;

    public RetryRule(String errorEquals, Long intervalSeconds, Long maxAttempts, Double backoffRate,
                     Long maxDelaySeconds, String jitterStrategy) {
        if (errorEquals == null || errorEquals.isEmpty()) {
            throw new IllegalArgumentException("RetryRule requires an ErrorEquals entry");
        }
        this.errorEquals = List.of(errorEquals);
        this.intervalSeconds = intervalSeconds;
        this.maxAttempts = maxAttempts;
        this.backoffRate = backoffRate;
        this.maxDelaySeconds = maxDelaySeconds;
        this.jitterStrategy = jitterStrategy;
    }

    public List<String> getErrorEquals() {
        return errorEquals;
    }

    public Long getIntervalSeconds() {
        return intervalSeconds;
    }

    public Long getMaxAttempts() {
        return maxAttempts;
    }

    public Double getBackoffRate() {
        return backoffRate;
    }

    public Long getMaxDelaySeconds() {
        return maxDelaySeconds;
    }

    public String getJitterStrategy() {
        return jitterStrategy;
    }
}
