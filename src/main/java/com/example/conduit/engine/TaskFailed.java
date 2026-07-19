package com.example.conduit.engine;

/** A Task's worker (or a timeout) reported failure. A trigger record; drives Retry/Catch/fail. */
public record TaskFailed(String state, String error, String cause) implements EngineEvent {
}
