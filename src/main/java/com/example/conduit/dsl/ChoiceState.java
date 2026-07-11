package com.example.conduit.dsl;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Choice: first matching rule wins → its {@code Next}; no match → {@code Default}. Instant. */
public record ChoiceState(
        @JsonProperty("Choices") List<ChoiceRule> choices,
        @JsonProperty("Default") String defaultNext
) implements State {
}
