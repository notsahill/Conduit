package com.example.conduit.engine;

import com.example.conduit.dsl.DslParser;
import com.example.conduit.dsl.WorkflowGraph;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure fan-out/fan-in: Parallel spawns children, parks, and aggregates ordered outputs. */
class EngineParallelTest {

    private static final String PARALLEL = """
            { "StartAt": "P",
              "States": {
                "P": { "Type": "Parallel", "End": true,
                       "Branches": [
                         { "StartAt": "B0", "States": { "B0": { "Type": "Pass", "End": true } } },
                         { "StartAt": "B1", "States": { "B1": { "Type": "Pass", "End": true } } }
                       ] }
              } }
            """;

    private final WorkflowGraph graph = DslParser.parse(PARALLEL);
    private final Object input = Map.of("k", "v");

    @Test
    void entrySpawnsChildrenAndParks() {
        ExecutionStarted start = new ExecutionStarted(input);

        DecideResult result = Engine.decide(graph, Replay.replay(List.of(start)), start);

        assertThat(result.events()).contains(new StateEntered("P"), new ChildrenSpawned("P", 2));
        assertThat(result.commands()).containsExactly(new SpawnChildren("P", List.of(input, input)));
        assertThat(result.events()).noneMatch(ExecutionSucceeded.class::isInstance);
    }

    @Test
    void firstChildDoneKeepsWaiting() {
        ChildSucceeded first = new ChildSucceeded("P", 0, "a");
        List<EngineEvent> log = List.of(new ExecutionStarted(input),
                new StateEntered("P"), new ChildrenSpawned("P", 2), first);

        DecideResult result = Engine.decide(graph, Replay.replay(log), first);

        assertThat(result.events()).isEmpty();
        assertThat(result.commands()).isEmpty();
    }

    @Test
    void lastChildDoneAggregatesOrderedAndCompletes() {
        ChildSucceeded second = new ChildSucceeded("P", 1, "b");
        List<EngineEvent> log = List.of(new ExecutionStarted(input),
                new StateEntered("P"), new ChildrenSpawned("P", 2),
                new ChildSucceeded("P", 0, "a"), second);

        DecideResult result = Engine.decide(graph, Replay.replay(log), second);

        assertThat(result.events()).containsExactly(
                new StateExited("P", List.of("a", "b")),
                new ExecutionSucceeded(List.of("a", "b")));
        assertThat(result.commands()).containsExactly(
                new CompleteExecution(com.example.conduit.enums.ExecutionStatus.SUCCEEDED,
                        List.of("a", "b"), null));
    }

    @Test
    void childFailureWithoutCatchFailsExecution() {
        ChildFailed failed = new ChildFailed("P", 0, "Boom", "branch blew up");
        List<EngineEvent> log = List.of(new ExecutionStarted(input),
                new StateEntered("P"), new ChildrenSpawned("P", 2), failed);

        DecideResult result = Engine.decide(graph, Replay.replay(log), failed);

        assertThat(result.events()).containsExactly(new ExecutionFailed("Boom", "branch blew up"));
    }
}
