package com.example.conduit.dispatch;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Everything a worker needs to run one task attempt. Built by the engine on Task entry and handed to
 * a {@link TaskDispatcher}; the engine itself never touches Redis. {@code parameters} is reserved (v2).
 */
public record TaskContext(
        String taskId,
        String executionId,
        String stateName,
        String resource,
        int attempt,
        String idempotencyKey,
        JsonNode input,
        JsonNode parameters
) {
}
