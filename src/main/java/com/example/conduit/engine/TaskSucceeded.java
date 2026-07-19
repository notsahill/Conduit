package com.example.conduit.engine;

/**
 * A Task's worker reported success with {@code output} for {@code attempt}. A trigger record: replay
 * applies no data delta (the following {@code StateExited} carries it); {@code decide} reads
 * {@code output} directly. The attempt lets the engine drop a result from a superseded try.
 */
public record TaskSucceeded(String state, int attempt, Object output) implements EngineEvent {
}
