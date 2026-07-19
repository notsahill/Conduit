package com.example.conduit.engine;

import com.example.conduit.dsl.ChoiceRule;
import com.example.conduit.dsl.ChoiceState;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * Evaluates a {@link ChoiceState} against the current data: the first top-level rule that matches
 * wins and returns its {@code Next}; no match returns {@code Default} (possibly {@code null}, which
 * the engine turns into a {@code States.NoChoiceMatched} failure). Pure; no infra.
 */
final class ChoiceEvaluator {

    private ChoiceEvaluator() {
    }

    /** The Next of the first matching rule, else Default, else {@code null}. */
    static String evaluate(ChoiceState choice, Object data) {
        if (choice.choices() != null) {
            for (ChoiceRule rule : choice.choices()) {
                if (matches(rule, data)) {
                    return rule.next();
                }
            }
        }
        return choice.defaultNext();
    }

    private static boolean matches(ChoiceRule rule, Object data) {
        if (rule.and() != null) {
            return allMatch(rule.and(), data);
        }
        if (rule.or() != null) {
            return anyMatch(rule.or(), data);
        }
        if (rule.not() != null) {
            return !matches(rule.not(), data);
        }
        return matchesLeaf(rule, data);
    }

    private static boolean allMatch(List<ChoiceRule> rules, Object data) {
        return rules.stream().allMatch(r -> matches(r, data));
    }

    private static boolean anyMatch(List<ChoiceRule> rules, Object data) {
        return rules.stream().anyMatch(r -> matches(r, data));
    }

    private static boolean matchesLeaf(ChoiceRule rule, Object data) {
        JsonNode value = JsonPaths.resolve(data, rule.variable());

        if (rule.isPresent() != null) {
            boolean present = value != null && !value.isNull();
            return present == rule.isPresent();
        }
        if (value == null || value.isNull()) {
            return false; // any comparator against a missing value is false
        }
        if (rule.stringEquals() != null) {
            return value.isTextual() && value.asText().equals(rule.stringEquals());
        }
        if (rule.booleanEquals() != null) {
            return value.isBoolean() && value.asBoolean() == rule.booleanEquals();
        }
        if (rule.numericEquals() != null) {
            return value.isNumber() && value.asDouble() == rule.numericEquals();
        }
        if (rule.numericGreaterThan() != null) {
            return value.isNumber() && value.asDouble() > rule.numericGreaterThan();
        }
        if (rule.numericLessThan() != null) {
            return value.isNumber() && value.asDouble() < rule.numericLessThan();
        }
        if (rule.numericGreaterThanEquals() != null) {
            return value.isNumber() && value.asDouble() >= rule.numericGreaterThanEquals();
        }
        if (rule.numericLessThanEquals() != null) {
            return value.isNumber() && value.asDouble() <= rule.numericLessThanEquals();
        }
        return false;
    }
}
