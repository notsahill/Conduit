package com.example.conduit.engine;

/** A Wait state parked for {@code seconds}; the scheduler resumes it. Informational on replay. */
public record WaitStarted(String state, int seconds) implements EngineEvent {
}
