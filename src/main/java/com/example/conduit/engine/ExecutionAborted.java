package com.example.conduit.engine;

/** Terminal: the execution was stopped by an operator (StopExecution). Replay → ABORTED. */
public record ExecutionAborted(String cause) implements EngineEvent {
}
