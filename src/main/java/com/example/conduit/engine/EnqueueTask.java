package com.example.conduit.engine;

/** Route a Task to its resource for execution. {@code parameters} is reserved (v2), carried through. */
public record EnqueueTask(
        String state,
        String resource,
        int attempt,
        Object input,
        Object parameters,
        Integer timeoutSeconds
) implements Command {
}
