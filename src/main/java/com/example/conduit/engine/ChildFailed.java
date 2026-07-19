package com.example.conduit.engine;

/** Child execution {@code index} of a Parallel/Map state failed. A fan-in trigger; drives Catch/fail. */
public record ChildFailed(String state, int index, String error, String cause) implements EngineEvent {
}
