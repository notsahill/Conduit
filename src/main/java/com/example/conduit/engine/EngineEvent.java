package com.example.conduit.engine;

/**
 * A domain event in the immutable execution log. {@link Replay} folds these into an
 * {@link ExecutionState}; {@link Engine#decide} both consumes them (as triggers) and emits them.
 * Sealed so replay switches exhaustively. This is the engine-layer event, distinct from the
 * {@code ExecutionEvent} JPA entity it will be persisted as (Phase 3).
 */
public sealed interface EngineEvent
        permits ExecutionStarted, StateEntered, StateExited, TaskScheduled,
        TaskSucceeded, TaskFailed, ExecutionSucceeded, ExecutionFailed,
        WaitStarted, WaitCompleted, RetryScheduled, RetryDue, TaskTimedOut,
        ChoiceEvaluated, ChildrenSpawned, ChildSucceeded, ChildFailed, ExecutionAborted {
}
