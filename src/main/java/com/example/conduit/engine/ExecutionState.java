package com.example.conduit.engine;

import com.example.conduit.enums.ExecutionStatus;

/**
 * The derived state of an execution — a pure function of its event log (see {@link Replay}). Never
 * persisted as truth; the {@code executions} projection columns are a cache of these fields.
 *
 * @param status           lifecycle status; {@code null} before {@code ExecutionStarted}
 * @param currentStateName the state the execution is parked at, or {@code null}
 * @param currentData      the JSON data flowing output → input between states
 */
public record ExecutionState(
        ExecutionStatus status,
        String currentStateName,
        Object currentData
) {
    /** The empty state before any event is applied. */
    public static ExecutionState initial() {
        return new ExecutionState(null, null, null);
    }
}
