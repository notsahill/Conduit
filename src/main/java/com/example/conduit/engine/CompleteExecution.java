package com.example.conduit.engine;

import com.example.conduit.enums.ExecutionStatus;

/** Finalize the execution: write terminal status and output/error to the projection. */
public record CompleteExecution(ExecutionStatus status, Object output, Object error)
        implements Command {
}
