package com.example.conduit.engine;

/** The execution began with {@code input} as the initial data. Always the first event. */
public record ExecutionStarted(Object input) implements EngineEvent {
}
