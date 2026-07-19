package com.example.conduit.engine;

/**
 * Control left {@code state} producing {@code output}, which becomes the current data. This is the
 * single data-mutation event: replay updates the flowing data only here.
 */
public record StateExited(String state, Object output) implements EngineEvent {
}
