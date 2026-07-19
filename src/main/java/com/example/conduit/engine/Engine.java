package com.example.conduit.engine;

import com.example.conduit.dsl.FailState;
import com.example.conduit.dsl.PassState;
import com.example.conduit.dsl.State;
import com.example.conduit.dsl.SucceedState;
import com.example.conduit.dsl.TaskState;
import com.example.conduit.dsl.WorkflowGraph;
import com.example.conduit.enums.ExecutionStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The pure decision core. {@code decide(graph, state, trigger)} computes the events to append and
 * commands to dispatch, with zero IO — fully unit-testable in memory. Instant states chain within a
 * single call until a parking state (Task/Wait/Parallel/Map) or a terminal is reached.
 */
public final class Engine {

    private Engine() {
    }

    public static DecideResult decide(WorkflowGraph graph, ExecutionState state, EngineEvent trigger) {
        List<EngineEvent> events = new ArrayList<>();
        List<Command> commands = new ArrayList<>();

        switch (trigger) {
            case ExecutionStarted ignored ->
                    enterState(graph, graph.startAt(), state.currentData(), events, commands);
            case TaskSucceeded ts -> onTaskSucceeded(graph, ts, events, commands);
            case TaskFailed tf -> failExecution(events, commands, tf.error(), tf.cause());
            default -> throw new UnsupportedOperationException("trigger not handled yet: " + trigger);
        }
        return new DecideResult(events, commands);
    }

    /** A task reported success: record the data it produced, then transition to its Next (or end). */
    private static void onTaskSucceeded(WorkflowGraph graph, TaskSucceeded ts,
                                        List<EngineEvent> events, List<Command> commands) {
        events.add(new StateExited(ts.state(), ts.output()));
        TaskState task = (TaskState) graph.states().get(ts.state());
        if (task.end()) {
            complete(events, commands, ts.output());
        } else {
            enterState(graph, task.next(), ts.output(), events, commands);
        }
    }

    private static void complete(List<EngineEvent> events, List<Command> commands, Object output) {
        events.add(new ExecutionSucceeded(output));
        commands.add(new CompleteExecution(ExecutionStatus.SUCCEEDED, output, null));
    }

    private static void failExecution(List<EngineEvent> events, List<Command> commands,
                                      String error, String cause) {
        events.add(new ExecutionFailed(error, cause));
        commands.add(new CompleteExecution(ExecutionStatus.FAILED, null,
                Map.of("Error", error == null ? "" : error, "Cause", cause == null ? "" : cause)));
    }

    private static void enterState(WorkflowGraph graph, String name, Object data,
                                   List<EngineEvent> events, List<Command> commands) {
        State state = graph.states().get(name);
        events.add(new StateEntered(name));
        switch (state) {
            case TaskState task -> {
                int attempt = 1;
                events.add(new TaskScheduled(name, task.resource(), attempt, data));
                commands.add(new EnqueueTask(name, task.resource(), attempt, data,
                        task.parameters(), task.timeoutSeconds()));
            }
            case PassState pass -> {
                // Instant: inject static Result or pass input through, then chain to Next / terminal.
                Object output = pass.result() != null ? pass.result() : data;
                events.add(new StateExited(name, output));
                if (pass.end()) {
                    complete(events, commands, output);
                } else {
                    enterState(graph, pass.next(), output, events, commands);
                }
            }
            case SucceedState ignored -> complete(events, commands, data);
            case FailState fail -> failExecution(events, commands, fail.error(), fail.cause());
            default -> throw new UnsupportedOperationException("state not handled yet: " + state);
        }
    }
}
