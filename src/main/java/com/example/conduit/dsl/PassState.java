package com.example.conduit.dsl;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Pass: injects a static {@code Result} or passes input through, then transitions. Instant. */
public record PassState(
        @JsonProperty("Result") Object result,
        @JsonProperty("Next") String next,
        @JsonProperty("End") boolean end
) implements State {
}
