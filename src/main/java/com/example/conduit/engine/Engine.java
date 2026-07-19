package com.example.conduit.engine;

import com.example.conduit.dsl.Catcher;
import com.example.conduit.dsl.FailState;
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
            case SucceedState ignored -> complete(events, commands, data);
            case FailState fail -> failExecution(events, commands, fail.error(), fail.cause());
            default -> throw new UnsupportedOperationException("state not handled yet: " + s);
        }
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
        JsonNode node = dotPath(data, wait.secondsPath());
        if (node == null || !node.isNumber()) {
            throw new IllegalStateException("Wait SecondsPath '" + wait.secondsPath() + "' did not resolve a number");
        }
        return node.asInt();
    }

    /** Minimal dot-path lookup ({@code $.a.b}) over the JSON data. Full JSONPath is out of scope (v2). */
    private static JsonNode dotPath(Object data, String path) {
        if (!(data instanceof JsonNode node) || path == null) {
            return null;
        }
        String p = path.startsWith("$.") ? path.substring(2) : path.startsWith("$") ? path.substring(1) : path;
        for (String segment : p.split("\\.")) {
            if (segment.isEmpty()) {
                continue;
            }
            node = node.get(segment);
            if (node == null) {
                return null;
            }
        }
        return node;
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
