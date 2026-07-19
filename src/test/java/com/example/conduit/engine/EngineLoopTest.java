package com.example.conduit.engine;

import com.example.conduit.dsl.DslParser;
import com.example.conduit.dsl.WorkflowGraph;
import com.example.conduit.enums.ExecutionStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Phase 2 "done when": drive a linear Task → Pass → Succeed machine entirely in memory. Fake
 * events in, events/commands out — no Redis, no DB. Proves the replay↔decide round-trip: every
 * event {@code decide} emits folds back through {@code replay} to the expected terminal state.
 */
class EngineLoopTest {

    private static final String MACHINE = """
            {
              "StartAt": "Ocr",
              "States": {
                "Ocr":      { "Type": "Task", "Resource": "ocr-handler", "Next": "Classify" },
                "Classify": { "Type": "Pass", "Next": "Done" },
                "Done":     { "Type": "Succeed" }
              }
            }
            """;

    @Test
    void drivesLinearMachineToCompletion() {
        WorkflowGraph graph = DslParser.parse(MACHINE);
        Object input = Map.of("doc", "invoice.pdf");
        Object ocrOutput = Map.of("text", "total: 42");

        List<EngineEvent> log = new ArrayList<>();
        List<Command> allCommands = new ArrayList<>();

        // Trigger 1 — start. Engine enters the first Task and dispatches it, then parks.
        ExecutionStarted start = new ExecutionStarted(input);
        log.add(start);
        drive(graph, log, allCommands, start);

        assertThat(Replay.replay(log).status()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(allCommands).last().isInstanceOf(EnqueueTask.class);

        // Trigger 2 — the worker reports success. Engine flows through the instant Pass to Succeed.
        TaskSucceeded taskDone = new TaskSucceeded("Ocr", 1, ocrOutput);
        log.add(taskDone);
        drive(graph, log, allCommands, taskDone);

        ExecutionState finalState = Replay.replay(log);
        assertThat(finalState.status()).isEqualTo(ExecutionStatus.SUCCEEDED);
        assertThat(finalState.currentStateName()).isEqualTo("Done");
        assertThat(finalState.currentData()).isEqualTo(ocrOutput);

        assertThat(allCommands).hasSize(2);
        assertThat(allCommands.get(0)).isInstanceOf(EnqueueTask.class);
        assertThat(allCommands.get(1)).isEqualTo(
                new CompleteExecution(ExecutionStatus.SUCCEEDED, ocrOutput, null));
    }

    /** Simulates the engine loop's transactional core: replay, decide, append emitted events. */
    private void drive(WorkflowGraph graph, List<EngineEvent> log, List<Command> allCommands,
                       EngineEvent trigger) {
        DecideResult result = Engine.decide(graph, Replay.replay(log), trigger);
        log.addAll(result.events());
        allCommands.addAll(result.commands());
    }
}
