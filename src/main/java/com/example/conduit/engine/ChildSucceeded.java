package com.example.conduit.engine;

/**
 * Child execution {@code index} of a Parallel/Map state finished with {@code output}. A fan-in trigger;
 * replay records the output at its index so aggregation stays ordered.
 */
public record ChildSucceeded(String state, int index, Object output) implements EngineEvent {
}
