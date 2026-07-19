package com.example.conduit.engine;

/**
 * A task's timeout fired for {@code attempt}. A trigger record; handled like a failure with error
 * {@code States.Timeout}. The attempt lets the engine ignore a timeout for an already-superseded try.
 */
public record TaskTimedOut(String state, int attempt) implements EngineEvent {
}
