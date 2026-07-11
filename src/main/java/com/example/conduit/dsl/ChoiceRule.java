package com.example.conduit.dsl;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * One Choice rule: a comparison on {@code Variable} (dot-path) against an operator, or a boolean
 * composition via {@code And}/{@code Or}/{@code Not} of nested rules. Top-level rules carry
 * {@code Next}; nested composition rules do not. Operator evaluation lands in Phase 6.
 */
public record ChoiceRule(
        @JsonProperty("Variable") String variable,
        @JsonProperty("StringEquals") String stringEquals,
        @JsonProperty("NumericEquals") Double numericEquals,
        @JsonProperty("NumericGreaterThan") Double numericGreaterThan,
        @JsonProperty("NumericLessThan") Double numericLessThan,
        @JsonProperty("NumericGreaterThanEquals") Double numericGreaterThanEquals,
        @JsonProperty("NumericLessThanEquals") Double numericLessThanEquals,
        @JsonProperty("BooleanEquals") Boolean booleanEquals,
        @JsonProperty("IsPresent") Boolean isPresent,
        @JsonProperty("And") List<ChoiceRule> and,
        @JsonProperty("Or") List<ChoiceRule> or,
        @JsonProperty("Not") ChoiceRule not,
        @JsonProperty("Next") String next
) {
}
