package com.example.conduit.dsl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowGraphValidatorTest {

    private final WorkflowGraphValidator validator = new WorkflowGraphValidator();

    private WorkflowGraph parse(String json) {
        return DslParser.parse(json);
    }

    @Test
    void acceptsValidLinearWorkflow() {
        WorkflowGraph graph = parse("""
                {
                  "StartAt": "First",
                  "States": {
                    "First": { "Type": "Pass", "Next": "Done" },
                    "Done":  { "Type": "Succeed" }
                  }
                }
                """);

        assertThatCode(() -> validator.validate(graph)).doesNotThrowAnyException();
    }

    @Test
    void rejectsStartAtThatDoesNotExist() {
        WorkflowGraph graph = parse("""
                {
                  "StartAt": "Missing",
                  "States": { "Done": { "Type": "Succeed" } }
                }
                """);

        assertThatThrownBy(() -> validator.validate(graph))
                .isInstanceOf(DslValidationException.class)
                .hasMessageContaining("StartAt")
                .hasMessageContaining("Missing");
    }

    @Test
    void rejectsNextThatDoesNotResolve() {
        WorkflowGraph graph = parse("""
                {
                  "StartAt": "First",
                  "States": {
                    "First": { "Type": "Pass", "Next": "Nowhere" }
                  }
                }
                """);

        assertThatThrownBy(() -> validator.validate(graph))
                .isInstanceOf(DslValidationException.class)
                .hasMessageContaining("Nowhere");
    }

    @Test
    void rejectsWhenNoTerminalReachable() {
        // Task loops to itself; parking state (not instant), but no Succeed/Fail is ever reachable.
        WorkflowGraph graph = parse("""
                {
                  "StartAt": "Loop",
                  "States": {
                    "Loop": { "Type": "Task", "Resource": "r", "Next": "Loop" }
                  }
                }
                """);

        assertThatThrownBy(() -> validator.validate(graph))
                .isInstanceOf(DslValidationException.class)
                .hasMessageContaining("terminal");
    }

    @Test
    void rejectsInstantStateCycle() {
        // A (Choice) -> B (Pass) -> A ... both instant, so decide() would spin. A terminal (Done) is
        // still reachable via Default, so this isolates the cycle rule from the reachability rule.
        WorkflowGraph graph = parse("""
                {
                  "StartAt": "A",
                  "States": {
                    "A": { "Type": "Choice", "Choices": [{ "Variable": "$.x", "StringEquals": "y", "Next": "B" }], "Default": "Done" },
                    "B": { "Type": "Pass", "Next": "A" },
                    "Done": { "Type": "Succeed" }
                  }
                }
                """);

        assertThatThrownBy(() -> validator.validate(graph))
                .isInstanceOf(DslValidationException.class)
                .hasMessageContaining("cycle");
    }

    @Test
    void acceptsCycleThroughParkingState() {
        // A (Task, parking) -> B (Choice) -> A is a legal loop: the Task parks each iteration, so
        // decide() cannot spin. Terminal reachable via Default.
        WorkflowGraph graph = parse("""
                {
                  "StartAt": "A",
                  "States": {
                    "A": { "Type": "Task", "Resource": "r", "Next": "B" },
                    "B": { "Type": "Choice", "Choices": [{ "Variable": "$.again", "BooleanEquals": true, "Next": "A" }], "Default": "Done" },
                    "Done": { "Type": "Succeed" }
                  }
                }
                """);

        assertThatCode(() -> validator.validate(graph)).doesNotThrowAnyException();
    }

    @Test
    void rejectsTaskWithoutResource() {
        WorkflowGraph graph = parse("""
                {
                  "StartAt": "X",
                  "States": {
                    "X": { "Type": "Task", "Next": "Done" },
                    "Done": { "Type": "Succeed" }
                  }
                }
                """);

        assertThatThrownBy(() -> validator.validate(graph))
                .isInstanceOf(DslValidationException.class)
                .hasMessageContaining("Resource");
    }

    @Test
    void rejectsFailWithoutError() {
        WorkflowGraph graph = parse("""
                {
                  "StartAt": "X",
                  "States": { "X": { "Type": "Fail", "Cause": "boom" } }
                }
                """);

        assertThatThrownBy(() -> validator.validate(graph))
                .isInstanceOf(DslValidationException.class)
                .hasMessageContaining("Error");
    }

    @Test
    void rejectsNonTerminalStateWithoutNextOrEnd() {
        WorkflowGraph graph = parse("""
                {
                  "StartAt": "X",
                  "States": { "X": { "Type": "Pass" } }
                }
                """);

        assertThatThrownBy(() -> validator.validate(graph))
                .isInstanceOf(DslValidationException.class)
                .hasMessageContaining("Next or End");
    }

    @Test
    void rejectsWaitWithoutSecondsOrSecondsPath() {
        WorkflowGraph graph = parse("""
                {
                  "StartAt": "Hold",
                  "States": {
                    "Hold": { "Type": "Wait", "Next": "Done" },
                    "Done": { "Type": "Succeed" }
                  }
                }
                """);

        assertThatThrownBy(() -> validator.validate(graph))
                .isInstanceOf(DslValidationException.class)
                .hasMessageContaining("Seconds");
    }

    @Test
    void acceptsEndTrueAsTerminal() {
        WorkflowGraph graph = parse("""
                {
                  "StartAt": "X",
                  "States": { "X": { "Type": "Pass", "End": true } }
                }
                """);

        assertThatCode(() -> validator.validate(graph)).doesNotThrowAnyException();
    }
}
