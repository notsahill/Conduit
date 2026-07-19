package com.example.conduit.engine;

import java.util.List;

/**
 * Fan out one child execution per input for a Parallel/Map state. The dispatcher creates the child
 * rows (recording parent id, branch index, and the parent state so the child's sub-graph resolves)
 * and starts each. {@code inputs} is ordered; index i becomes branch/iteration i.
 */
public record SpawnChildren(String state, List<Object> inputs) implements Command {
}
