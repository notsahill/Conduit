package com.example.conduit.dsl;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Parallel: run every branch (each a nested workflow) on the same input as a child execution.
 * All succeed → output = list of branch outputs; any fail → Retry/Catch, else fail.
 */
public record ParallelState(
        @JsonProperty("Branches") List<WorkflowGraph> branches,
        @JsonProperty("Retry") List<Retrier> retry,
        @JsonProperty("Catch") List<Catcher> catchers,
        @JsonProperty("Next") String next,
        @JsonProperty("End") boolean end
) implements State {
}
