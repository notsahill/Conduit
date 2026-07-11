package com.example.conduit.dsl;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** A Catch rule: on a matching error, jump to {@code Next} (error injected into data). */
public record Catcher(
        @JsonProperty("ErrorEquals") List<String> errorEquals,
        @JsonProperty("Next") String next
) {
}
