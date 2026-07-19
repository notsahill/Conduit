package com.example.conduit.engine;

/**
 * A task exhausted its retries with no matching Catch: route it to the dead-letter stream for
 * inspection/redrive. The execution is failed in the same decision.
 */
public record SendToDlq(String state, int attempt, String error, String cause, Object input) implements Command {
}
