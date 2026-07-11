package com.example.conduit.dsl;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DslParserTest {

    @Test
    void parsesLinearWorkflow() {
        String json = """
                {
                  "StartAt": "First",
                  "States": {
                    "First": { "Type": "Pass", "Next": "Done" },
                    "Done":  { "Type": "Succeed" }
                  }
                }
                """;

        WorkflowGraph graph = DslParser.parse(json);

        assertThat(graph.startAt()).isEqualTo("First");
        assertThat(graph.states()).containsOnlyKeys("First", "Done");
        assertThat(graph.states().get("First")).isInstanceOf(PassState.class);
        assertThat(graph.states().get("Done")).isInstanceOf(SucceedState.class);
        assertThat(((PassState) graph.states().get("First")).next()).isEqualTo("Done");
    }

    @Test
    void parsesTaskWithRetryCatchAndTimeout() {
        String json = """
                {
                  "StartAt": "Ocr",
                  "States": {
                    "Ocr": {
                      "Type": "Task",
                      "Resource": "ocr-handler",
                      "TimeoutSeconds": 30,
                      "Parameters": { "lang": "en" },
                      "Retry": [{ "ErrorEquals": ["TransientError"], "IntervalSeconds": 5, "MaxAttempts": 3, "BackoffRate": 2.0 }],
                      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "HandleFailure" }],
                      "Next": "Done"
                    },
                    "HandleFailure": { "Type": "Fail", "Error": "IngestFailed", "Cause": "ocr exhausted" },
                    "Done": { "Type": "Succeed" }
                  }
                }
                """;

        WorkflowGraph graph = DslParser.parse(json);

        TaskState task = (TaskState) graph.states().get("Ocr");
        assertThat(task.resource()).isEqualTo("ocr-handler");
        assertThat(task.timeoutSeconds()).isEqualTo(30);
        assertThat(task.next()).isEqualTo("Done");
        assertThat(task.parameters()).isNotNull();
        assertThat(task.retry()).singleElement().satisfies(r -> {
            assertThat(r.errorEquals()).containsExactly("TransientError");
            assertThat(r.intervalSeconds()).isEqualTo(5);
            assertThat(r.maxAttempts()).isEqualTo(3);
            assertThat(r.backoffRate()).isEqualTo(2.0);
        });
        assertThat(task.catchers()).singleElement().satisfies(c -> {
            assertThat(c.errorEquals()).containsExactly("States.ALL");
            assertThat(c.next()).isEqualTo("HandleFailure");
        });

        FailState fail = (FailState) graph.states().get("HandleFailure");
        assertThat(fail.error()).isEqualTo("IngestFailed");
        assertThat(fail.cause()).isEqualTo("ocr exhausted");
    }

    @Test
    void parsesWaitState() {
        String json = """
                {
                  "StartAt": "Hold",
                  "States": {
                    "Hold": { "Type": "Wait", "Seconds": 10, "Next": "Done" },
                    "Done": { "Type": "Succeed" }
                  }
                }
                """;

        WaitState wait = (WaitState) DslParser.parse(json).states().get("Hold");
        assertThat(wait.seconds()).isEqualTo(10);
        assertThat(wait.secondsPath()).isNull();
        assertThat(wait.next()).isEqualTo("Done");
    }

    @Test
    void parsesParallelWithNestedBranches() {
        String json = """
                {
                  "StartAt": "Fork",
                  "States": {
                    "Fork": {
                      "Type": "Parallel",
                      "Branches": [
                        { "StartAt": "A", "States": { "A": { "Type": "Succeed" } } },
                        { "StartAt": "B", "States": { "B": { "Type": "Succeed" } } }
                      ],
                      "Next": "Done"
                    },
                    "Done": { "Type": "Succeed" }
                  }
                }
                """;

        ParallelState parallel = (ParallelState) DslParser.parse(json).states().get("Fork");
        assertThat(parallel.branches()).hasSize(2);
        assertThat(parallel.branches().get(0).startAt()).isEqualTo("A");
        assertThat(parallel.branches().get(0).states()).containsKey("A");
        assertThat(parallel.next()).isEqualTo("Done");
    }

    @Test
    void parsesMapWithIterator() {
        String json = """
                {
                  "StartAt": "Each",
                  "States": {
                    "Each": {
                      "Type": "Map",
                      "ItemsPath": "$.items",
                      "MaxConcurrency": 4,
                      "Iterator": { "StartAt": "Handle", "States": { "Handle": { "Type": "Succeed" } } },
                      "Next": "Done"
                    },
                    "Done": { "Type": "Succeed" }
                  }
                }
                """;

        MapState map = (MapState) DslParser.parse(json).states().get("Each");
        assertThat(map.itemsPath()).isEqualTo("$.items");
        assertThat(map.maxConcurrency()).isEqualTo(4);
        assertThat(map.iterator().startAt()).isEqualTo("Handle");
        assertThat(map.next()).isEqualTo("Done");
    }

    @Test
    void parsesChoiceWithOperatorAndDefault() {
        String json = """
                {
                  "StartAt": "Route",
                  "States": {
                    "Route": {
                      "Type": "Choice",
                      "Choices": [{ "Variable": "$.docType", "StringEquals": "invoice", "Next": "Invoice" }],
                      "Default": "Generic"
                    },
                    "Invoice": { "Type": "Succeed" },
                    "Generic": { "Type": "Succeed" }
                  }
                }
                """;

        ChoiceState choice = (ChoiceState) DslParser.parse(json).states().get("Route");
        assertThat(choice.defaultNext()).isEqualTo("Generic");
        assertThat(choice.choices()).singleElement().satisfies(rule -> {
            assertThat(rule.variable()).isEqualTo("$.docType");
            assertThat(rule.stringEquals()).isEqualTo("invoice");
            assertThat(rule.next()).isEqualTo("Invoice");
        });
    }

    @Test
    void rejectsUnknownStateType() {
        String json = """
                { "StartAt": "X", "States": { "X": { "Type": "Bogus" } } }
                """;

        assertThatThrownBy(() -> DslParser.parse(json)).isInstanceOf(DslParseException.class);
    }
}
