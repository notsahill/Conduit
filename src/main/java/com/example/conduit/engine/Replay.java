package com.example.conduit.engine;

import com.example.conduit.enums.ExecutionStatus;

import java.util.HashMap;
import java.util.LinkedHashMap;
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
            case ExecutionStarted e -> with(state).status(ExecutionStatus.RUNNING).currentState(null).data(e.input()).build();
            case StateEntered e -> with(state).currentState(e.state()).build();
            case StateExited e -> with(state).data(e.output()).build();
            case TaskScheduled e -> with(state).attempts(incremented(state.taskAttempts(), e.state())).build();
            case ExecutionSucceeded e -> with(state).status(ExecutionStatus.SUCCEEDED).data(e.output()).build();
            case ExecutionFailed ignored -> with(state).status(ExecutionStatus.FAILED).build();
            case ExecutionAborted ignored -> with(state).status(ExecutionStatus.ABORTED).build();
            case ChildrenSpawned e -> with(state).childProgress(
                    put(state.childProgress(), e.state(), new ChildProgress(e.count(), Map.of(), false, null, null))).build();
            case ChildSucceeded e -> with(state).childProgress(recordChild(state, e.state(), e.index(), e.output())).build();
            case ChildFailed e -> with(state).childProgress(failChild(state, e.state(), e.error(), e.cause())).build();
            // Trigger / informational records carry no replay delta.
            case TaskSucceeded ignored -> state;
            case TaskFailed ignored -> state;
            case WaitStarted ignored -> state;
            case WaitCompleted ignored -> state;
            case RetryScheduled ignored -> state;
            case RetryDue ignored -> state;
            case TaskTimedOut ignored -> state;
            case ChoiceEvaluated ignored -> state;
        };
    }

    private static Map<String, Integer> incremented(Map<String, Integer> attempts, String state) {
        Map<String, Integer> copy = new HashMap<>(attempts);
        copy.merge(state, 1, Integer::sum);
        return Map.copyOf(copy);
    }

    private static Map<String, ChildProgress> put(Map<String, ChildProgress> progress, String state, ChildProgress value) {
        Map<String, ChildProgress> copy = new HashMap<>(progress);
        copy.put(state, value);
        return Map.copyOf(copy);
    }

    private static Map<String, ChildProgress> recordChild(ExecutionState state, String name, int index, Object output) {
        ChildProgress current = state.childProgressOf(name);
        Map<Integer, Object> outputs = new LinkedHashMap<>(current.outputs());
        outputs.put(index, output);
        return put(state.childProgress(), name,
                new ChildProgress(current.total(), Map.copyOf(outputs), current.failed(), current.error(), current.cause()));
    }

    private static Map<String, ChildProgress> failChild(ExecutionState state, String name, String error, String cause) {
        ChildProgress current = state.childProgressOf(name);
        return put(state.childProgress(), name,
                new ChildProgress(current.total(), current.outputs(), true, error, cause));
    }

    // Minimal fluent copier to keep the fold readable across five fields.
    private static Builder with(ExecutionState s) {
        return new Builder(s);
    }

    private static final class Builder {
        private ExecutionStatus status;
        private String currentState;
        private Object data;
        private Map<String, Integer> attempts;
        private Map<String, ChildProgress> childProgress;

        Builder(ExecutionState s) {
            this.status = s.status();
            this.currentState = s.currentStateName();
            this.data = s.currentData();
            this.attempts = s.taskAttempts();
            this.childProgress = s.childProgress();
        }

        Builder status(ExecutionStatus v) { this.status = v; return this; }
        Builder currentState(String v) { this.currentState = v; return this; }
        Builder data(Object v) { this.data = v; return this; }
        Builder attempts(Map<String, Integer> v) { this.attempts = v; return this; }
        Builder childProgress(Map<String, ChildProgress> v) { this.childProgress = v; return this; }

        ExecutionState build() {
            return new ExecutionState(status, currentState, data, attempts, childProgress);
        }
    }
}
