package com.example.conduit.dsl;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Map: run {@code Iterator} (a nested workflow) once per array item at {@code ItemsPath}, each item
 * the child input. Output = list of iteration outputs in order. {@code MaxConcurrency} throttles fan-out.
 */
public record MapState(
        @JsonProperty("ItemsPath") String itemsPath,
        @JsonProperty("Iterator") WorkflowGraph iterator,
        @JsonProperty("MaxConcurrency") Integer maxConcurrency,
        @JsonProperty("Retry") List<Retrier> retry,
        @JsonProperty("Catch") List<Catcher> catchers,
        @JsonProperty("Next") String next,
        @JsonProperty("End") boolean end
) implements State {
}
