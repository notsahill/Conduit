package com.example.conduit.dsl;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/** Parsed workflow definition: an entry point plus a map of state name to state. */
public record WorkflowGraph(
        @JsonProperty("StartAt") String startAt,
        @JsonProperty("States") Map<String, State> states
) {
}
