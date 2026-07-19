package com.example.conduit.engine;

import com.example.conduit.enums.ExecutionStatus;

import java.util.Map;

/**
 * The derived state of an execution — a pure function of its event log (see {@link Replay}). Never
 * persisted as truth; the {@code executions} projection columns are a cache of these fields.
 *
 * @param status           lifecycle status; {@code null} before {@code ExecutionStarted}
 * @param currentStateName the state the execution is parked at, or {@code null}
 * @param currentData      the JSON data flowing output → input between states
 * @param taskAttempts     scheduled-attempt count per state (how many times a task was dispatched)
 * @param childProgress    fan-in bookkeeping per Parallel/Map state
 */
public record ExecutionState(
        ExecutionStatus status,
        String currentStateName,
        Object currentData,
        Map<String, Integer> taskAttempts,
        Map<String, ChildProgress> childProgress
) {
    /** The empty state before any event is applied. */
    public static ExecutionState initial() {
        return new ExecutionState(null, null, null, Map.of(), Map.of());
    }

    /** The scheduled-attempt count for a state (0 if never dispatched). */
    public int attemptOf(String state) {
        return taskAttempts.getOrDefault(state, 0);
    }

    public ChildProgress childProgressOf(String state) {
        return childProgress.get(state);
    }
}
