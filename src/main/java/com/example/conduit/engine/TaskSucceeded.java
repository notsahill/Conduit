package com.example.conduit.engine;

/**
 * A Task's worker reported success with {@code output}. A trigger record: replay applies no data
 * delta (the following {@code StateExited} carries it); {@code decide} reads {@code output} directly.
 */
public record TaskSucceeded(String state, Object output) implements EngineEvent {
}
