package com.example.conduit.engine;

/** A Choice state routed to {@code next}. Informational (audit); replay applies no delta. */
public record ChoiceEvaluated(String state, String next) implements EngineEvent {
}
