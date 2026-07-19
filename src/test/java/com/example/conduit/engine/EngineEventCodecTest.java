package com.example.conduit.engine;

import com.example.conduit.enums.EventType;
import com.example.conduit.model.ExecutionEvent;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The codec bridges the in-memory {@link EngineEvent} to the persisted {@link ExecutionEvent} row and
 * back. Round-tripping every event type is what lets the engine loop rebuild state from the DB log.
 */
class EngineEventCodecTest {

    private final EngineEventCodec codec = new EngineEventCodec(new JsonMapper());

    @Test
    void roundTripsEveryEventType() {
        List<EngineEvent> events = List.of(
                new ExecutionStarted(new TextNode("in")),
                new StateEntered("Ocr"),
                new StateExited("Ocr", new TextNode("out")),
                new TaskScheduled("Ocr", "ocr-handler", 1, new IntNode(7)),
                new TaskSucceeded("Ocr", new TextNode("done")),
                new TaskFailed("Ocr", "BadThing", "boom"),
                new ExecutionSucceeded(new TextNode("final")),
                new ExecutionFailed("States.TaskFailed", "gave up"));

        for (EngineEvent original : events) {
            ExecutionEvent row = codec.toRow("exec-1", 3, original);
            assertThat(codec.fromRow(row))
                    .as("round-trip of %s", original)
                    .isEqualTo(original);
        }
    }

    @Test
    void mapsTypeAndStateNameOntoRow() {
        ExecutionEvent row = codec.toRow("exec-1", 5, new TaskScheduled("Ocr", "r", 2, null));
        assertThat(row.getExecutionId()).isEqualTo("exec-1");
        assertThat(row.getSeq()).isEqualTo(5);
        assertThat(row.getType()).isEqualTo(EventType.TASK_SCHEDULED);
        assertThat(row.getStateName()).isEqualTo("Ocr");
    }

    @Test
    void executionStartedHasNoStateName() {
        ExecutionEvent row = codec.toRow("exec-1", 0, new ExecutionStarted(new TextNode("x")));
        assertThat(row.getStateName()).isNull();
        assertThat(row.getType()).isEqualTo(EventType.EXECUTION_STARTED);
    }
}
