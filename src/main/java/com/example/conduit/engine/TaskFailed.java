package com.example.conduit.engine;

/**
 * A Task's worker reported failure for {@code attempt}. A trigger record; drives Retry/Catch/fail.
 * The attempt lets the engine drop a failure from a superseded try.
 */
public record TaskFailed(String state, int attempt, String error, String cause) implements EngineEvent {
}
