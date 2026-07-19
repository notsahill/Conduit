package com.example.conduit.engine;

/** Terminal: the execution failed with an {@code error} name and {@code cause}. */
public record ExecutionFailed(String error, String cause) implements EngineEvent {
}
