package com.example.conduit.engine;

/** A failed attempt will be retried as {@code attempt} after {@code seconds} backoff. Informational. */
public record RetryScheduled(String state, int attempt, int seconds) implements EngineEvent {
}
