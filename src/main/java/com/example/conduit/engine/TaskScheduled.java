package com.example.conduit.engine;

/** A Task was dispatched (attempt {@code attempt}) and the execution is parked awaiting its result. */
public record TaskScheduled(String state, String resource, int attempt, Object input)
        implements EngineEvent {
}
