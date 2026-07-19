package com.example.conduit.engine;

import com.example.conduit.enums.ExecutionStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ReplayTest {

    @Test
    void replaysExecutionStartedToRunning() {
        ExecutionState state = Replay.replay(List.of(
                new ExecutionStarted(Map.of("doc", "invoice.pdf"))
        ));

        assertThat(state.status()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(state.currentData()).isEqualTo(Map.of("doc", "invoice.pdf"));
        assertThat(state.currentStateName()).isNull();
    }

    @Test
    void replaysLinearSequenceToSucceeded() {
        Object input = Map.of("doc", "invoice.pdf");
        Object out = Map.of("text", "hello");

        ExecutionState state = Replay.replay(List.of(
                new ExecutionStarted(input),
                new StateEntered("Ocr"),
                new TaskScheduled("Ocr", "ocr-handler", 1, input),
                new TaskSucceeded("Ocr", 1, out),
                new StateExited("Ocr", out),
                new StateEntered("Done"),
                new ExecutionSucceeded(out)
        ));

        assertThat(state.status()).isEqualTo(ExecutionStatus.SUCCEEDED);
        assertThat(state.currentStateName()).isEqualTo("Done");
        assertThat(state.currentData()).isEqualTo(out);
    }
}
