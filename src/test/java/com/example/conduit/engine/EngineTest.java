package com.example.conduit.engine;

import com.example.conduit.dsl.DslParser;
import com.example.conduit.dsl.WorkflowGraph;
import com.example.conduit.enums.ExecutionStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class EngineTest {

    private static final String LINEAR = """
            {
              "StartAt": "Ocr",
              "States": {
                "Ocr":  { "Type": "Task", "Resource": "ocr-handler", "Next": "Done" },
                "Done": { "Type": "Succeed" }
              }
            }
            """;

    private final WorkflowGraph linear = DslParser.parse(LINEAR);

    @Test
    void startEntersFirstTaskAndDispatches() {
        Object input = Map.of("doc", "invoice.pdf");
        ExecutionStarted started = new ExecutionStarted(input);
        ExecutionState state = Replay.replay(List.of(started));

        DecideResult result = Engine.decide(linear, state, started);

        assertThat(result.events()).containsExactly(
                new StateEntered("Ocr"),
                new TaskScheduled("Ocr", "ocr-handler", 1, input)
        );
        assertThat(result.commands()).singleElement()
                .isInstanceOfSatisfying(EnqueueTask.class, cmd -> {
                    assertThat(cmd.state()).isEqualTo("Ocr");
                    assertThat(cmd.resource()).isEqualTo("ocr-handler");
                    assertThat(cmd.attempt()).isEqualTo(1);
                    assertThat(cmd.input()).isEqualTo(input);
                });
        // No terminal event emitted — the execution parks at the task.
        assertThat(result.events()).noneMatch(e -> e instanceof ExecutionSucceeded);
        assertThat(state.status()).isEqualTo(ExecutionStatus.RUNNING);
    }

    @Test
    void taskSuccessTransitionsToNextAndCompletes() {
        Object input = Map.of("doc", "invoice.pdf");
        Object out = Map.of("text", "hello");
        TaskSucceeded trigger = new TaskSucceeded("Ocr", 1, out);
        ExecutionState state = Replay.replay(List.of(
                new ExecutionStarted(input),
                new StateEntered("Ocr"),
                new TaskScheduled("Ocr", "ocr-handler", 1, input),
                trigger
        ));

        DecideResult result = Engine.decide(linear, state, trigger);

        assertThat(result.events()).containsExactly(
                new StateExited("Ocr", out),
                new StateEntered("Done"),
                new ExecutionSucceeded(out)
        );
        assertThat(result.commands()).containsExactly(
                new CompleteExecution(ExecutionStatus.SUCCEEDED, out, null)
        );
    }

    @Test
    void instantPassChainsThroughToTerminalInOneDecide() {
        String json = """
                {
                  "StartAt": "Ocr",
                  "States": {
                    "Ocr":  { "Type": "Task", "Resource": "ocr-handler", "Next": "Tag" },
                    "Tag":  { "Type": "Pass", "Result": { "tagged": true }, "Next": "Done" },
                    "Done": { "Type": "Succeed" }
                  }
                }
                """;
        WorkflowGraph graph = DslParser.parse(json);
        Object out = Map.of("text", "hello");
        Object tagged = Map.of("tagged", true);
        TaskSucceeded trigger = new TaskSucceeded("Ocr", 1, out);

        DecideResult result = Engine.decide(graph, Replay.replay(List.of(trigger)), trigger);

        // Task exit → Pass (instant, injects Result) → Succeed, all within one decide.
        assertThat(result.events()).containsExactly(
                new StateExited("Ocr", out),
                new StateEntered("Tag"),
                new StateExited("Tag", tagged),
                new StateEntered("Done"),
                new ExecutionSucceeded(tagged)
        );
        assertThat(result.commands()).containsExactly(
                new CompleteExecution(ExecutionStatus.SUCCEEDED, tagged, null)
        );
    }

    @Test
    void taskFailureWithoutRetryOrCatchFailsExecutionAndDeadLetters() {
        Object input = Map.of("doc", "x");
        TaskFailed trigger = new TaskFailed("Ocr", 1, "TransientError", "ocr blew up");
        ExecutionState state = Replay.replay(List.of(
                new ExecutionStarted(input),
                new StateEntered("Ocr"),
                new TaskScheduled("Ocr", "ocr-handler", 1, input),
                trigger
        ));

        DecideResult result = Engine.decide(linear, state, trigger);

        assertThat(result.events()).containsExactly(
                new ExecutionFailed("TransientError", "ocr blew up")
        );
        assertThat(result.commands()).anySatisfy(cmd ->
                assertThat(cmd).isInstanceOfSatisfying(CompleteExecution.class, c ->
                        assertThat(c.status()).isEqualTo(ExecutionStatus.FAILED)));
        assertThat(result.commands()).anySatisfy(cmd ->
                assertThat(cmd).isInstanceOfSatisfying(SendToDlq.class, dlq -> {
                    assertThat(dlq.state()).isEqualTo("Ocr");
                    assertThat(dlq.error()).isEqualTo("TransientError");
                }));
    }
}
