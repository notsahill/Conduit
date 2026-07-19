package com.example.conduit.engine;

import com.example.conduit.dsl.ChoiceState;
import com.example.conduit.dsl.DslParser;
import com.example.conduit.dsl.WorkflowGraph;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure Choice evaluation: operators, And/Or/Not composition, dot-path Variable, and Default. */
class ChoiceEvaluatorTest {

    private static final JsonMapper MAPPER = new JsonMapper();

    private ChoiceState choice(String rulesJson) {
        WorkflowGraph graph = DslParser.parse("""
                { "StartAt": "C",
                  "States": {
                    "C": { "Type": "Choice", %s },
                    "Yes": { "Type": "Succeed" },
                    "No": { "Type": "Succeed" },
                    "Fallback": { "Type": "Succeed" }
                  } }
                """.formatted(rulesJson));
        return (ChoiceState) graph.states().get("C");
    }

    private JsonNode data(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void stringEqualsRoutesToMatchingRule() {
        ChoiceState c = choice("""
                "Choices": [{ "Variable": "$.docType", "StringEquals": "invoice", "Next": "Yes" }],
                "Default": "Fallback"
                """);
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"docType\": \"invoice\" }"))).isEqualTo("Yes");
    }

    @Test
    void noMatchFallsThroughToDefault() {
        ChoiceState c = choice("""
                "Choices": [{ "Variable": "$.docType", "StringEquals": "invoice", "Next": "Yes" }],
                "Default": "Fallback"
                """);
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"docType\": \"receipt\" }"))).isEqualTo("Fallback");
    }

    @Test
    void noMatchNoDefaultReturnsNull() {
        ChoiceState c = choice("""
                "Choices": [{ "Variable": "$.n", "NumericEquals": 1, "Next": "Yes" }]
                """);
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"n\": 2 }"))).isNull();
    }

    @Test
    void numericComparators() {
        ChoiceState c = choice("""
                "Choices": [{ "Variable": "$.amount", "NumericGreaterThan": 100, "Next": "Yes" }],
                "Default": "No"
                """);
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"amount\": 150 }"))).isEqualTo("Yes");
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"amount\": 50 }"))).isEqualTo("No");
    }

    @Test
    void andRequiresAllConditions() {
        ChoiceState c = choice("""
                "Choices": [{ "And": [
                    { "Variable": "$.a", "StringEquals": "x" },
                    { "Variable": "$.n", "NumericLessThan": 10 }
                ], "Next": "Yes" }],
                "Default": "No"
                """);
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"a\": \"x\", \"n\": 5 }"))).isEqualTo("Yes");
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"a\": \"x\", \"n\": 50 }"))).isEqualTo("No");
    }

    @Test
    void orRequiresAnyCondition() {
        ChoiceState c = choice("""
                "Choices": [{ "Or": [
                    { "Variable": "$.a", "BooleanEquals": true },
                    { "Variable": "$.b", "BooleanEquals": true }
                ], "Next": "Yes" }],
                "Default": "No"
                """);
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"a\": false, \"b\": true }"))).isEqualTo("Yes");
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"a\": false, \"b\": false }"))).isEqualTo("No");
    }

    @Test
    void notNegates() {
        ChoiceState c = choice("""
                "Choices": [{ "Not": { "Variable": "$.a", "StringEquals": "x" }, "Next": "Yes" }],
                "Default": "No"
                """);
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"a\": \"y\" }"))).isEqualTo("Yes");
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"a\": \"x\" }"))).isEqualTo("No");
    }

    @Test
    void isPresentChecksPathResolution() {
        ChoiceState c = choice("""
                "Choices": [{ "Variable": "$.opt", "IsPresent": true, "Next": "Yes" }],
                "Default": "No"
                """);
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"opt\": 1 }"))).isEqualTo("Yes");
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"other\": 1 }"))).isEqualTo("No");
    }

    @Test
    void nestedDotPathResolves() {
        ChoiceState c = choice("""
                "Choices": [{ "Variable": "$.meta.kind", "StringEquals": "pdf", "Next": "Yes" }],
                "Default": "No"
                """);
        assertThat(ChoiceEvaluator.evaluate(c, data("{ \"meta\": { \"kind\": \"pdf\" } }"))).isEqualTo("Yes");
    }
}
