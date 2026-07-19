package com.example.conduit.api.dto;

import com.example.conduit.enums.TaskStatus;
import com.example.conduit.enums.TaskType;
import com.example.conduit.model.Task;

import java.time.Instant;

/** Debug read model for the task/timer rows behind an execution. */
public record TaskView(
        String id,
        String stateName,
        TaskType type,
        TaskStatus status,
        String timerKind,
        Integer attempt,
        String resource,
        Instant nextRunAt,
        Object input
) {
    public static TaskView of(Task t) {
        return new TaskView(t.getId(), t.getStateName(), t.getType(), t.getStatus(), t.getTimerKind(),
                t.getAttempt(), t.getResource(), t.getNextRunAt(), t.getInput());
    }
}
