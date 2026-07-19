package com.example.conduit.engine;

/** Terminal: the execution completed successfully with {@code output}. */
public record ExecutionSucceeded(Object output) implements EngineEvent {
}
