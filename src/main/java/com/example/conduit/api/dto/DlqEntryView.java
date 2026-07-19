package com.example.conduit.api.dto;

/** One dead-letter entry from the {@code dlq} stream: a task that exhausted retries with no Catch. */
public record DlqEntryView(
        String id,
        String executionId,
        String stateName,
        String attempt,
        String error,
        String cause
) {
}
