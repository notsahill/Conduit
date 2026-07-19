package com.example.conduit.api.dto;

import com.example.conduit.enums.ExecutionStatus;
import com.example.conduit.model.Execution;

import java.time.Instant;

/** Read model for DescribeExecution: the projection row (a cache of replayed state). */
public record ExecutionView(
        String id,
        String workflowDefinitionId,
        String name,
        ExecutionStatus status,
        String currentState,
        Object input,
        Object output,
        Object error,
        Instant startedAt,
        Instant stoppedAt
) {
    public static ExecutionView of(Execution e) {
        return new ExecutionView(
                e.getId(), e.getWorkflowDefinitionId(), e.getName(), e.getStatus(), e.getCurrentState(),
                e.getInput(), e.getOutput(), e.getError(), e.getStartedAt(), e.getStoppedAt());
    }
}
