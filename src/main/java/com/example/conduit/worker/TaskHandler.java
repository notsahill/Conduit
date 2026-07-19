package com.example.conduit.worker;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * User-written unit of work for a resource. Receives the task input, returns the output (which
 * becomes the next state's input). Must be idempotent: a retry is a new attempt with a new key, so a
 * handler that performed a side effect before timing out may be invoked again.
 */
@FunctionalInterface
public interface TaskHandler {
    JsonNode handle(JsonNode input) throws Exception;
}
