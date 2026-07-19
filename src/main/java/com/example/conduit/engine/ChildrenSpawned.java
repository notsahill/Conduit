package com.example.conduit.engine;

/**
 * A Parallel/Map state fanned out {@code count} child executions. Replay uses it to know how many
 * child completions to await before fanning in.
 */
public record ChildrenSpawned(String state, int count) implements EngineEvent {
}
