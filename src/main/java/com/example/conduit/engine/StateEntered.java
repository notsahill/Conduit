package com.example.conduit.engine;

/** Control entered {@code state}. */
public record StateEntered(String state) implements EngineEvent {
}
