package com.example.conduit.engine;

/** The Wait timer elapsed. A trigger record: replay applies no delta; decide moves to Next. */
public record WaitCompleted(String state) implements EngineEvent {
}
