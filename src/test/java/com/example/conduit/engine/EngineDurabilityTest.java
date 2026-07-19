package com.example.conduit.engine;

import com.example.conduit.dsl.DslParser;
import com.example.conduit.dsl.WorkflowGraph;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure-engine coverage of the Phase 5 decisions: Wait, Retry + backoff, Catch, Timeout. */
class EngineDurabilityTest {

    private static final String MACHINE = """
            { "StartAt": "A",
              "States": {
                "A": { "Type": "Task", "Resource": "r", "TimeoutSeconds": 30,
                       "Retry": [{ "ErrorEquals": ["Transient"], "IntervalSeconds": 2, "MaxAttempts": 3, "BackoffRate": 2.0 }],
                       "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "Handle" }],
                       "Next": "W" },
                "W":      { "Type": "Wait", "Seconds": 5, "Next": "Done" },
                "Handle": { "Type": "Pass", "Next": "Done" },
                "Done":   { "Type": "Succeed" }
              } }
            """;

    private final WorkflowGraph graph = DslParser.parse(MACHINE);
    private final Object input = Map.of("k", "v");

    @Test
    void enteringTaskArmsTimeoutTimer() {
        ExecutionStarted start = new ExecutionStarted(input);
        DecideResult result = Engine.decide(graph, Replay.replay(List.of(start)), start);

        assertThat(result.commands()).contains(new ScheduleTimer("A", TimerKind.TIMEOUT, 30, 1));
    }

    @Test
    void enteringWaitSchedulesTimerAndParks() {
        TaskSucceeded ok = new TaskSucceeded("A", 1, Map.of("x", 1));
        List<EngineEvent> log = List.of(new ExecutionStarted(input),
                new StateEntered("A"), new TaskScheduled("A", "r", 1, input), ok);

        DecideResult result = Engine.decide(graph, Replay.replay(log), ok);

        assertThat(result.events()).contains(new StateEntered("W"), new WaitStarted("W", 5));
        assertThat(result.commands()).contains(new ScheduleTimer("W", TimerKind.WAIT, 5, 0));
        assertThat(result.events()).noneMatch(ExecutionSucceeded.class::isInstance);
    }

    @Test
    void waitCompletedTransitionsToNext() {
        WaitCompleted done = new WaitCompleted("W");
        List<EngineEvent> log = List.of(new ExecutionStarted(input),
                new StateEntered("A"), new TaskScheduled("A", "r", 1, input),
                new StateExited("A", input), new StateEntered("W"), new WaitStarted("W", 5), done);

        DecideResult result = Engine.decide(graph, Replay.replay(log), done);

        assertThat(result.events()).contains(new StateEntered("Done"));
        assertThat(result.events()).anyMatch(ExecutionSucceeded.class::isInstance);
    }

    @Test
    void firstFailureSchedulesRetryWithBackoff() {
        TaskFailed fail = new TaskFailed("A", 1, "Transient", "boom");
        List<EngineEvent> log = List.of(new ExecutionStarted(input),
                new StateEntered("A"), new TaskScheduled("A", "r", 1, input), fail);

        DecideResult result = Engine.decide(graph, Replay.replay(log), fail);

        // attempt 1 fails → backoff = 2 * 2^0 = 2s; retry scheduled for attempt 2.
        assertThat(result.events()).containsExactly(new RetryScheduled("A", 2, 2));
        assertThat(result.commands()).containsExactly(new ScheduleTimer("A", TimerKind.RETRY, 2, 2));
    }

    @Test
    void secondFailureBackoffGrowsExponentially() {
        TaskFailed fail = new TaskFailed("A", 2, "Transient", "boom");
        List<EngineEvent> log = List.of(new ExecutionStarted(input),
                new StateEntered("A"), new TaskScheduled("A", "r", 1, input),
                new TaskScheduled("A", "r", 2, input), fail);

        DecideResult result = Engine.decide(graph, Replay.replay(log), fail);

        // attempt 2 fails → backoff = 2 * 2^1 = 4s; retry scheduled for attempt 3.
        assertThat(result.commands()).containsExactly(new ScheduleTimer("A", TimerKind.RETRY, 4, 3));
    }

    @Test
    void retryDueRedispatchesAtNextAttempt() {
        RetryDue due = new RetryDue("A");
        List<EngineEvent> log = List.of(new ExecutionStarted(input),
                new StateEntered("A"), new TaskScheduled("A", "r", 1, input), due);

        DecideResult result = Engine.decide(graph, Replay.replay(log), due);

        assertThat(result.events()).containsExactly(new TaskScheduled("A", "r", 2, input));
        assertThat(result.commands()).contains(
                new EnqueueTask("A", "r", 2, input, null, 30),
                new ScheduleTimer("A", TimerKind.TIMEOUT, 30, 2));
    }

    @Test
    void exhaustedRetriesFallThroughToCatch() {
        TaskFailed fail = new TaskFailed("A", 3, "Transient", "boom");
        List<EngineEvent> log = List.of(new ExecutionStarted(input),
                new StateEntered("A"),
                new TaskScheduled("A", "r", 1, input),
                new TaskScheduled("A", "r", 2, input),
                new TaskScheduled("A", "r", 3, input), fail);

        DecideResult result = Engine.decide(graph, Replay.replay(log), fail);

        // attempt 3 == MaxAttempts → no more retries → Catch (States.ALL) routes to Handle → Done.
        assertThat(result.events()).contains(new StateEntered("Handle"));
        assertThat(result.events()).anyMatch(ExecutionSucceeded.class::isInstance);
    }

    @Test
    void timeoutIsHandledLikeAFailureAndCaught() {
        TaskTimedOut timeout = new TaskTimedOut("A", 1);
        List<EngineEvent> log = List.of(new ExecutionStarted(input),
                new StateEntered("A"), new TaskScheduled("A", "r", 1, input), timeout);

        DecideResult result = Engine.decide(graph, Replay.replay(log), timeout);

        // States.Timeout doesn't match the "Transient" retrier → Catch (States.ALL) → Handle.
        assertThat(result.events()).contains(new StateEntered("Handle"));
    }
}
