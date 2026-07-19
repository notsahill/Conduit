package com.example.conduit.engine;

import com.example.conduit.dsl.Catcher;
import com.example.conduit.dsl.ChoiceState;
import com.example.conduit.dsl.FailState;
import com.example.conduit.dsl.MapState;
import com.example.conduit.dsl.ParallelState;
import com.example.conduit.dsl.PassState;
import com.example.conduit.dsl.Retrier;
import com.example.conduit.dsl.State;
import com.example.conduit.dsl.SucceedState;
import com.example.conduit.dsl.TaskState;
import com.example.conduit.dsl.WaitState;
import com.example.conduit.dsl.WorkflowGraph;
import com.example.conduit.enums.ExecutionStatus;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The pure decision core. {@code decide(graph, state, trigger)} computes the events to append and
 * commands to dispatch, with zero IO — fully unit-testable in memory. Instant states chain within a
 * single call until a parking state (Task/Wait) or a terminal is reached; a runtime cap backstops the
 * validator's instant-cycle rejection.
 */
public final class Engine {

    static final int MAX_INSTANT_TRANSITIONS = 1000;
    static final String TIMEOUT_ERROR = "States.Timeout";
    static final String INFINITE_LOOP_ERROR = "States.InfiniteLoop";
    static final String NO_CHOICE_MATCHED_ERROR = "States.NoChoiceMatched";
    private static final String ALL_ERRORS = "States.ALL";

    private Engine() {
    }

    public static DecideResult decide(WorkflowGraph graph, ExecutionState state, EngineEvent trigger) {
        List<EngineEvent> events = new ArrayList<>();
        List<Command> commands = new ArrayList<>();

        switch (trigger) {
            case ExecutionStarted ignored ->
                    enterState(graph, graph.startAt(), state.currentData(), state, events, commands, 0);
            case TaskSucceeded ts -> onTaskSucceeded(graph, ts, state, events, commands);
            case TaskFailed tf -> onFailure(graph, tf.state(), tf.error(), tf.cause(), state, events, commands);
            case TaskTimedOut tt ->
                    onFailure(graph, tt.state(), TIMEOUT_ERROR, "task timed out", state, events, commands);
            case WaitCompleted wc -> onWaitCompleted(graph, wc.state(), state, events, commands);
            case RetryDue rd -> redispatchTask(graph, rd.state(), state, events, commands);
            case ChildSucceeded cs -> onChildSucceeded(graph, cs, state, events, commands);
            case ChildFailed cf -> onChildFailed(graph, cf, state, events, commands);
            default -> throw new UnsupportedOperationException("trigger not handled yet: " + trigger);
        }
        return new DecideResult(events, commands);
    }

    private static void enterState(WorkflowGraph graph, String name, Object data, ExecutionState state,
                                   List<EngineEvent> events, List<Command> commands, int depth) {
        if (depth > MAX_INSTANT_TRANSITIONS) {
            failExecution(events, commands, INFINITE_LOOP_ERROR, "exceeded instant-transition cap");
            return;
        }
        State s = graph.states().get(name);
        events.add(new StateEntered(name));
        switch (s) {
            case TaskState task -> dispatchTask(name, task, state.attemptOf(name) + 1, data, events, commands);
            case PassState pass -> {
                Object output = pass.result() != null ? pass.result() : data;
                events.add(new StateExited(name, output));
                if (pass.end()) {
                    complete(events, commands, output);
                } else {
                    enterState(graph, pass.next(), output, state, events, commands, depth + 1);
                }
            }
            case WaitState wait -> {
                int seconds = waitSeconds(wait, data);
                events.add(new WaitStarted(name, seconds));
                commands.add(new ScheduleTimer(name, TimerKind.WAIT, seconds, 0));
            }
            case ChoiceState choice -> {
                String next = ChoiceEvaluator.evaluate(choice, data);
                if (next == null) {
                    failExecution(events, commands, NO_CHOICE_MATCHED_ERROR,
                            "no Choice rule matched and no Default");
                } else {
                    events.add(new ChoiceEvaluated(name, next));
                    enterState(graph, next, data, state, events, commands, depth + 1);
                }
            }
            case ParallelState parallel -> {
                int branches = parallel.branches() == null ? 0 : parallel.branches().size();
                List<Object> inputs = new ArrayList<>();
                for (int i = 0; i < branches; i++) {
                    inputs.add(data); // every branch gets the same input
                }
                fanOut(graph, name, parallel.next(), parallel.end(), inputs, state, events, commands, depth);
            }
            case MapState map -> {
                JsonNode items = JsonPaths.resolve(data, map.itemsPath());
                if (items == null || !items.isArray()) {
                    failExecution(events, commands, "States.Runtime",
                            "Map ItemsPath '" + map.itemsPath() + "' did not resolve an array");
                } else {
                    List<Object> inputs = new ArrayList<>();
                    items.forEach(inputs::add); // each array item is one iteration's input
                    fanOut(graph, name, map.next(), map.end(), inputs, state, events, commands, depth);
                }
            }
            case SucceedState ignored -> complete(events, commands, data);
            case FailState fail -> failExecution(events, commands, fail.error(), fail.cause());
        }
    }

    /** Spawn one child per input and park; with no inputs, aggregate an empty result immediately. */
    private static void fanOut(WorkflowGraph graph, String name, String next, boolean end,
                               List<Object> inputs, ExecutionState state,
                               List<EngineEvent> events, List<Command> commands, int depth) {
        if (inputs.isEmpty()) {
            aggregate(graph, name, next, end, List.of(), state, events, commands, depth);
        } else {
            events.add(new ChildrenSpawned(name, inputs.size()));
            commands.add(new SpawnChildren(name, inputs));
        }
    }

    /** All children of a Parallel/Map finished: emit the ordered aggregate and move on. */
    private static void aggregate(WorkflowGraph graph, String name, String next, boolean end,
                                  List<Object> outputs, ExecutionState state,
                                  List<EngineEvent> events, List<Command> commands, int depth) {
        events.add(new StateExited(name, outputs));
        if (end) {
            complete(events, commands, outputs);
        } else {
            enterState(graph, next, outputs, state, events, commands, depth + 1);
        }
    }

    private static void onChildSucceeded(WorkflowGraph graph, ChildSucceeded cs, ExecutionState state,
                                         List<EngineEvent> events, List<Command> commands) {
        ChildProgress progress = state.childProgressOf(cs.state()); // includes this child (trigger replayed)
        if (progress.allSucceeded()) {
            State s = graph.states().get(cs.state());
            aggregate(graph, cs.state(), nextOf(s), endOf(s), orderedOutputs(progress),
                    state, events, commands, 0);
        }
        // otherwise still waiting on siblings — park with no further events
    }

    private static void onChildFailed(WorkflowGraph graph, ChildFailed cf, ExecutionState state,
                                      List<EngineEvent> events, List<Command> commands) {
        State s = graph.states().get(cf.state());
        Catcher catcher = matchCatch(catchersOf(s), cf.error());
        if (catcher != null) {
            Object errorOutput = errorData(cf.error(), cf.cause());
            events.add(new StateExited(cf.state(), errorOutput));
            enterState(graph, catcher.next(), errorOutput, state, events, commands, 0);
        } else {
            failExecution(events, commands, cf.error(), cf.cause());
        }
    }

    private static List<Object> orderedOutputs(ChildProgress progress) {
        List<Object> ordered = new ArrayList<>(progress.total());
        for (int i = 0; i < progress.total(); i++) {
            ordered.add(progress.outputs().get(i));
        }
        return ordered;
    }

    private static String nextOf(State s) {
        return switch (s) {
            case ParallelState p -> p.next();
            case MapState m -> m.next();
            default -> null;
        };
    }

    private static boolean endOf(State s) {
        return switch (s) {
            case ParallelState p -> p.end();
            case MapState m -> m.end();
            default -> false;
        };
    }

    private static List<Catcher> catchersOf(State s) {
        return switch (s) {
            case TaskState t -> t.catchers();
            case ParallelState p -> p.catchers();
            case MapState m -> m.catchers();
            default -> null;
        };
    }

    /** Dispatch a task attempt: schedule it, enqueue it, and arm its timeout if configured. */
    private static void dispatchTask(String name, TaskState task, int attempt, Object data,
                                     List<EngineEvent> events, List<Command> commands) {
        events.add(new TaskScheduled(name, task.resource(), attempt, data));
        commands.add(new EnqueueTask(name, task.resource(), attempt, data, task.parameters(), task.timeoutSeconds()));
        if (task.timeoutSeconds() != null) {
            commands.add(new ScheduleTimer(name, TimerKind.TIMEOUT, task.timeoutSeconds(), attempt));
        }
    }

    private static void redispatchTask(WorkflowGraph graph, String name, ExecutionState state,
                                       List<EngineEvent> events, List<Command> commands) {
        TaskState task = (TaskState) graph.states().get(name);
        dispatchTask(name, task, state.attemptOf(name) + 1, state.currentData(), events, commands);
    }

    private static void onTaskSucceeded(WorkflowGraph graph, TaskSucceeded ts, ExecutionState state,
                                        List<EngineEvent> events, List<Command> commands) {
        events.add(new StateExited(ts.state(), ts.output()));
        TaskState task = (TaskState) graph.states().get(ts.state());
        if (task.end()) {
            complete(events, commands, ts.output());
        } else {
            enterState(graph, task.next(), ts.output(), state, events, commands, 0);
        }
    }

    private static void onWaitCompleted(WorkflowGraph graph, String name, ExecutionState state,
                                        List<EngineEvent> events, List<Command> commands) {
        WaitState wait = (WaitState) graph.states().get(name);
        if (wait.end()) {
            complete(events, commands, state.currentData());
        } else {
            enterState(graph, wait.next(), state.currentData(), state, events, commands, 0);
        }
    }

    /** Task failure (or timeout): Retry if a rule matches and attempts remain, else Catch, else fail + DLQ. */
    private static void onFailure(WorkflowGraph graph, String name, String error, String cause,
                                  ExecutionState state, List<EngineEvent> events, List<Command> commands) {
        TaskState task = (TaskState) graph.states().get(name);
        int attempt = state.attemptOf(name);

        Retrier retrier = matchRetry(task.retry(), error);
        if (retrier != null && attempt < maxAttempts(retrier)) {
            int seconds = backoffSeconds(retrier, attempt);
            events.add(new RetryScheduled(name, attempt + 1, seconds));
            commands.add(new ScheduleTimer(name, TimerKind.RETRY, seconds, attempt + 1));
            return;
        }
        Catcher catcher = matchCatch(task.catchers(), error);
        if (catcher != null) {
            Object errorOutput = errorData(error, cause);
            events.add(new StateExited(name, errorOutput));
            enterState(graph, catcher.next(), errorOutput, state, events, commands, 0);
            return;
        }
        failExecution(events, commands, error, cause);
        commands.add(new SendToDlq(name, attempt, error, cause, state.currentData()));
    }

    private static Retrier matchRetry(List<Retrier> retriers, String error) {
        if (retriers == null) {
            return null;
        }
        return retriers.stream().filter(r -> matches(r.errorEquals(), error)).findFirst().orElse(null);
    }

    private static Catcher matchCatch(List<Catcher> catchers, String error) {
        if (catchers == null) {
            return null;
        }
        return catchers.stream().filter(c -> matches(c.errorEquals(), error)).findFirst().orElse(null);
    }

    private static boolean matches(List<String> errorEquals, String error) {
        return errorEquals != null && (errorEquals.contains(ALL_ERRORS) || errorEquals.contains(error));
    }

    private static int maxAttempts(Retrier retrier) {
        return retrier.maxAttempts() != null ? retrier.maxAttempts() : 1;
    }

    /** Exponential backoff: {@code interval * rate^(attempt-1)} seconds for the 1-based failed attempt. */
    private static int backoffSeconds(Retrier retrier, int attempt) {
        int interval = retrier.intervalSeconds() != null ? retrier.intervalSeconds() : 1;
        double rate = retrier.backoffRate() != null ? retrier.backoffRate() : 1.0;
        double seconds = interval * Math.pow(rate, Math.max(0, attempt - 1));
        return Math.max(0, (int) Math.round(seconds));
    }

    private static int waitSeconds(WaitState wait, Object data) {
        if (wait.seconds() != null) {
            return wait.seconds();
        }
        JsonNode node = JsonPaths.resolve(data, wait.secondsPath());
        if (node == null || !node.isNumber()) {
            throw new IllegalStateException("Wait SecondsPath '" + wait.secondsPath() + "' did not resolve a number");
        }
        return node.asInt();
    }

    private static Object errorData(String error, String cause) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("Error", error);
        data.put("Cause", cause);
        return data;
    }

    private static void complete(List<EngineEvent> events, List<Command> commands, Object output) {
        events.add(new ExecutionSucceeded(output));
        commands.add(new CompleteExecution(ExecutionStatus.SUCCEEDED, output, null));
    }

    private static void failExecution(List<EngineEvent> events, List<Command> commands, String error, String cause) {
        events.add(new ExecutionFailed(error, cause));
        commands.add(new CompleteExecution(ExecutionStatus.FAILED, null, errorData(error, cause)));
    }
}
