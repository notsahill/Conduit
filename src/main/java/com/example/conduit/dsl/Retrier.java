package com.example.conduit.dsl;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** A Retry rule: on a matching error, back off and re-run up to {@code MaxAttempts}. */
public record Retrier(
        @JsonProperty("ErrorEquals") List<String> errorEquals,
        @JsonProperty("IntervalSeconds") Integer intervalSeconds,
        @JsonProperty("MaxAttempts") Integer maxAttempts,
        @JsonProperty("BackoffRate") Double backoffRate
) {
}
