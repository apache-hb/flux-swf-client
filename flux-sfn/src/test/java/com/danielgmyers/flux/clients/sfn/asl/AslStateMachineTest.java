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
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AslStateMachineTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void rejectsUnknownStartAt() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new AslStateMachine(null, "DoesNotExist", null, null, Map.of("A", new SucceedState(null))));
    }

    @Test
    public void rejectsEmptyStates() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new AslStateMachine(null, "A", null, null, Map.of()));
    }

    @Test
    public void taskStateRequiresExactlyOneOfNextOrEnd() {
        // Neither Next nor End: invalid.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new TaskState(null, "arn:aws:states:::activity:thing", null, null, null, null, null, null, false));

        // Both Next and End=true: invalid.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new TaskState(null, "arn:aws:states:::activity:thing", null, null, null, null, null, "Next", true));
    }

    @Test
    public void serializedJsonHasExpectedAslShape() throws Exception {
        RetryRule retry = new RetryRule("retry", 5L, 3L, 2.0, null, RetryRule.JITTER_STRATEGY_FULL);
        TaskState task = new TaskState(null, "arn:aws:states:us-east-1:111111111111:activity:Foo-Bar",
                                       null, null, null, List.of(retry), null, null, true);
        AslStateMachine sm = new AslStateMachine("hi", "A", null, 123L, Map.of("A", task));

        JsonNode tree = MAPPER.readTree(sm.toJson());
        Assertions.assertEquals("hi", tree.get("Comment").asText());
        Assertions.assertEquals("A", tree.get("StartAt").asText());
        Assertions.assertEquals(123L, tree.get("TimeoutSeconds").asLong());
        JsonNode a = tree.get("States").get("A");
        Assertions.assertEquals("Task", a.get("Type").asText());
        Assertions.assertEquals(true, a.get("End").asBoolean());
        Assertions.assertEquals(1, a.get("Retry").size());
        JsonNode retryNode = a.get("Retry").get(0);
        Assertions.assertEquals("retry", retryNode.get("ErrorEquals").get(0).asText());
        Assertions.assertEquals(5L, retryNode.get("IntervalSeconds").asLong());
        Assertions.assertEquals(3L, retryNode.get("MaxAttempts").asLong());
        Assertions.assertEquals(2.0, retryNode.get("BackoffRate").asDouble(), 0.0001);
        Assertions.assertEquals("FULL", retryNode.get("JitterStrategy").asText());
    }

    @Test
    public void choiceStateRequiresNonEmptyChoices() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ChoiceState(null, List.of(), "X"));
    }

    @Test
    public void failStateSerializesErrorAndCause() throws Exception {
        AslStateMachine sm = new AslStateMachine(null, "F", null, null,
                Map.of("F", new FailState("err", "because", null)));
        JsonNode f = MAPPER.readTree(sm.toJson()).get("States").get("F");
        Assertions.assertEquals("Fail", f.get("Type").asText());
        Assertions.assertEquals("err", f.get("Error").asText());
        Assertions.assertEquals("because", f.get("Cause").asText());
    }

}
