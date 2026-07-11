package com.example.conduit.dsl;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Task: dispatch to a {@code Resource} and park until a result arrives. On success the output
 * becomes the data and control goes {@code Next}; on failure, Retry then Catch then fail.
 * {@code parameters} is reserved (parsed and carried, unused in v1).
 */
public record TaskState(
        @JsonProperty("Resource") String resource,
        @JsonProperty("Parameters") Object parameters,
        @JsonProperty("TimeoutSeconds") Integer timeoutSeconds,
        @JsonProperty("Retry") List<Retrier> retry,
        @JsonProperty("Catch") List<Catcher> catchers,
        @JsonProperty("Next") String next,
        @JsonProperty("End") boolean end
) implements State {
}
