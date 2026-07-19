package com.example.conduit.engine;

/** The retry backoff elapsed. A trigger record: decide re-dispatches the task at the next attempt. */
public record RetryDue(String state) implements EngineEvent {
}
