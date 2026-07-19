package com.example.conduit.engine;

import com.example.conduit.dsl.DslParser;
import com.example.conduit.dsl.WorkflowGraph;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Choice routing inside decide: instant evaluation, ChoiceEvaluated audit event, NoChoiceMatched. */
class EngineChoiceTest {

    private static final JsonMapper MAPPER = new JsonMapper();

    private static final String MACHINE = """
            { "StartAt": "Route",
              "States": {
                "Route": { "Type": "Choice",
                           "Choices": [{ "Variable": "$.t", "StringEquals": "a", "Next": "A" }],
                           "Default": "B" },
                "A": { "Type": "Pass", "End": true },
                "B": { "Type": "Pass", "End": true }
              } }
            """;

    @Test
    void routesToMatchingBranchInstantly() throws Exception {
        WorkflowGraph graph = DslParser.parse(MACHINE);
        Object input = MAPPER.readTree("{ \"t\": \"a\" }");
        ExecutionStarted start = new ExecutionStarted(input);

        DecideResult result = Engine.decide(graph, Replay.replay(List.of(start)), start);

        assertThat(result.events()).contains(
                new StateEntered("Route"), new ChoiceEvaluated("Route", "A"), new StateEntered("A"));
        assertThat(result.events()).anyMatch(ExecutionSucceeded.class::isInstance);
        assertThat(result.events()).noneMatch(e -> e instanceof StateEntered se && se.state().equals("B"));
    }

    @Test
    void fallsThroughToDefaultOnNoMatch() throws Exception {
        WorkflowGraph graph = DslParser.parse(MACHINE);
        Object input = MAPPER.readTree("{ \"t\": \"z\" }");
        ExecutionStarted start = new ExecutionStarted(input);

        DecideResult result = Engine.decide(graph, Replay.replay(List.of(start)), start);

        assertThat(result.events()).contains(new ChoiceEvaluated("Route", "B"), new StateEntered("B"));
    }

    @Test
    void noMatchAndNoDefaultFailsWithNoChoiceMatched() throws Exception {
        WorkflowGraph graph = DslParser.parse("""
                { "StartAt": "Route",
                  "States": {
                    "Route": { "Type": "Choice",
                               "Choices": [{ "Variable": "$.t", "StringEquals": "a", "Next": "A" }] },
                    "A": { "Type": "Pass", "End": true }
                  } }
                """);
        Object input = MAPPER.readTree("{ \"t\": \"z\" }");
        ExecutionStarted start = new ExecutionStarted(input);

        DecideResult result = Engine.decide(graph, Replay.replay(List.of(start)), start);

        assertThat(result.events()).contains(new ExecutionFailed("States.NoChoiceMatched",
                "no Choice rule matched and no Default"));
    }
}
