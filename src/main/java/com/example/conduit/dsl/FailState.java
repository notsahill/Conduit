package com.example.conduit.dsl;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Fail: terminal failure carrying an {@code Error} name and human-readable {@code Cause}. */
public record FailState(
        @JsonProperty("Error") String error,
        @JsonProperty("Cause") String cause
) implements State {
}
