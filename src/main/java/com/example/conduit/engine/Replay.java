package com.example.conduit.engine;

import com.example.conduit.enums.ExecutionStatus;

import java.util.List;

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
                    new ExecutionState(ExecutionStatus.RUNNING, null, e.input());
            case StateEntered e ->
                    new ExecutionState(state.status(), e.state(), state.currentData());
            case StateExited e ->
                    new ExecutionState(state.status(), state.currentStateName(), e.output());
            case ExecutionSucceeded e ->
                    new ExecutionState(ExecutionStatus.SUCCEEDED, state.currentStateName(), e.output());
            case ExecutionFailed ignored ->
                    new ExecutionState(ExecutionStatus.FAILED, state.currentStateName(), state.currentData());
            // Trigger/bookkeeping records: no state delta on replay.
            case TaskScheduled ignored -> state;
            case TaskSucceeded ignored -> state;
            case TaskFailed ignored -> state;
        };
    }
}
