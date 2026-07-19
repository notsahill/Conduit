package com.example.conduit.engine;

import com.example.conduit.enums.ExecutionStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Folds an event log into the current {@link ExecutionState}. Pure; the heart of event sourcing. */
public final class Replay {

    private Replay() {
    }

    public static ExecutionState replay(List<EngineEvent> events) {
        ExecutionState state = ExecutionState.initial();
        for (EngineEvent event : events) {
            state = apply(state, event);
        }
        return state;
    }

    private static ExecutionState apply(ExecutionState state, EngineEvent event) {
        return switch (event) {
            case ExecutionStarted e ->
                    new ExecutionState(ExecutionStatus.RUNNING, null, e.input(), state.taskAttempts());
            case StateEntered e ->
                    new ExecutionState(state.status(), e.state(), state.currentData(), state.taskAttempts());
            case StateExited e ->
                    new ExecutionState(state.status(), state.currentStateName(), e.output(), state.taskAttempts());
            case TaskScheduled e ->
                    new ExecutionState(state.status(), state.currentStateName(), state.currentData(),
                            incremented(state.taskAttempts(), e.state()));
            case ExecutionSucceeded e ->
                    new ExecutionState(ExecutionStatus.SUCCEEDED, state.currentStateName(), e.output(),
                            state.taskAttempts());
            case ExecutionFailed ignored ->
                    new ExecutionState(ExecutionStatus.FAILED, state.currentStateName(), state.currentData(),
                            state.taskAttempts());
            // Trigger / informational records carry no replay delta.
            case TaskSucceeded ignored -> state;
            case TaskFailed ignored -> state;
            case WaitStarted ignored -> state;
            case WaitCompleted ignored -> state;
            case RetryScheduled ignored -> state;
            case RetryDue ignored -> state;
            case TaskTimedOut ignored -> state;
        };
    }

    private static Map<String, Integer> incremented(Map<String, Integer> attempts, String state) {
        Map<String, Integer> copy = new HashMap<>(attempts);
        copy.merge(state, 1, Integer::sum);
        return Map.copyOf(copy);
    }
}
