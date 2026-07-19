package com.example.conduit.engine;

import com.example.conduit.enums.EventType;
import com.example.conduit.model.ExecutionEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

/**
 * Translates between the in-memory {@link EngineEvent} (what {@link Engine} and {@link Replay} speak)
 * and the persisted {@link ExecutionEvent} row. The type discriminator lives in a column; the
 * event-specific data lives in the JSONB payload. Round-trippable so the engine loop can rebuild
 * {@link ExecutionState} from the durable log.
 */
@Component
public class EngineEventCodec {

    private final ObjectMapper mapper;

    public EngineEventCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ExecutionEvent toRow(String executionId, int seq, EngineEvent event) {
        return ExecutionEvent.builder()
                .executionId(executionId)
                .seq(seq)
                .type(type(event))
                .stateName(stateName(event))
                .payload(payload(event))
                .build();
    }

    public EngineEvent fromRow(ExecutionEvent row) {
        JsonNode p = row.getPayload();
        String state = row.getStateName();
        return switch (row.getType()) {
            case EXECUTION_STARTED -> new ExecutionStarted(field(p, "input"));
            case STATE_ENTERED -> new StateEntered(state);
            case STATE_EXITED -> new StateExited(state, field(p, "output"));
            case TASK_SCHEDULED -> new TaskScheduled(state, p.get("resource").asText(),
                    p.get("attempt").asInt(), field(p, "input"));
            case TASK_SUCCEEDED -> new TaskSucceeded(state, p.get("attempt").asInt(), field(p, "output"));
            case TASK_FAILED -> new TaskFailed(state, p.get("attempt").asInt(), text(p, "error"), text(p, "cause"));
            case TASK_TIMED_OUT -> new TaskTimedOut(state, p.get("attempt").asInt());
            case WAIT_STARTED -> new WaitStarted(state, p.get("seconds").asInt());
            case WAIT_COMPLETED -> new WaitCompleted(state);
            case RETRY_SCHEDULED -> new RetryScheduled(state, p.get("attempt").asInt(), p.get("seconds").asInt());
            case RETRY_DUE -> new RetryDue(state);
            case CHOICE_EVALUATED -> new ChoiceEvaluated(state, text(p, "next"));
            case CHILDREN_SPAWNED -> new ChildrenSpawned(state, p.get("count").asInt());
            case CHILD_SUCCEEDED -> new ChildSucceeded(state, p.get("index").asInt(), field(p, "output"));
            case CHILD_FAILED -> new ChildFailed(state, p.get("index").asInt(), text(p, "error"), text(p, "cause"));
            case EXECUTION_SUCCEEDED -> new ExecutionSucceeded(field(p, "output"));
            case EXECUTION_FAILED -> new ExecutionFailed(text(p, "error"), text(p, "cause"));
            default -> throw new IllegalArgumentException("cannot decode event type " + row.getType());
        };
    }

    private EventType type(EngineEvent event) {
        return switch (event) {
            case ExecutionStarted ignored -> EventType.EXECUTION_STARTED;
            case StateEntered ignored -> EventType.STATE_ENTERED;
            case StateExited ignored -> EventType.STATE_EXITED;
            case TaskScheduled ignored -> EventType.TASK_SCHEDULED;
            case TaskSucceeded ignored -> EventType.TASK_SUCCEEDED;
            case TaskFailed ignored -> EventType.TASK_FAILED;
            case TaskTimedOut ignored -> EventType.TASK_TIMED_OUT;
            case WaitStarted ignored -> EventType.WAIT_STARTED;
            case WaitCompleted ignored -> EventType.WAIT_COMPLETED;
            case RetryScheduled ignored -> EventType.RETRY_SCHEDULED;
            case RetryDue ignored -> EventType.RETRY_DUE;
            case ChoiceEvaluated ignored -> EventType.CHOICE_EVALUATED;
            case ChildrenSpawned ignored -> EventType.CHILDREN_SPAWNED;
            case ChildSucceeded ignored -> EventType.CHILD_SUCCEEDED;
            case ChildFailed ignored -> EventType.CHILD_FAILED;
            case ExecutionSucceeded ignored -> EventType.EXECUTION_SUCCEEDED;
            case ExecutionFailed ignored -> EventType.EXECUTION_FAILED;
        };
    }

    private String stateName(EngineEvent event) {
        return switch (event) {
            case StateEntered e -> e.state();
            case StateExited e -> e.state();
            case TaskScheduled e -> e.state();
            case TaskSucceeded e -> e.state();
            case TaskFailed e -> e.state();
            case TaskTimedOut e -> e.state();
            case WaitStarted e -> e.state();
            case WaitCompleted e -> e.state();
            case RetryScheduled e -> e.state();
            case RetryDue e -> e.state();
            case ChoiceEvaluated e -> e.state();
            case ChildrenSpawned e -> e.state();
            case ChildSucceeded e -> e.state();
            case ChildFailed e -> e.state();
            case ExecutionStarted ignored -> null;
            case ExecutionSucceeded ignored -> null;
            case ExecutionFailed ignored -> null;
        };
    }

    private JsonNode payload(EngineEvent event) {
        ObjectNode p = mapper.createObjectNode();
        switch (event) {
            case ExecutionStarted e -> p.set("input", mapper.valueToTree(e.input()));
            case StateEntered ignored -> { /* state is a column; no payload */ }
            case StateExited e -> p.set("output", mapper.valueToTree(e.output()));
            case TaskScheduled e -> {
                p.put("resource", e.resource());
                p.put("attempt", e.attempt());
                p.set("input", mapper.valueToTree(e.input()));
            }
            case TaskSucceeded e -> {
                p.put("attempt", e.attempt());
                p.set("output", mapper.valueToTree(e.output()));
            }
            case TaskFailed e -> {
                p.put("attempt", e.attempt());
                p.put("error", e.error());
                p.put("cause", e.cause());
            }
            case TaskTimedOut e -> p.put("attempt", e.attempt());
            case WaitStarted e -> p.put("seconds", e.seconds());
            case WaitCompleted ignored -> { /* state is a column; no payload */ }
            case RetryScheduled e -> {
                p.put("attempt", e.attempt());
                p.put("seconds", e.seconds());
            }
            case RetryDue ignored -> { /* state is a column; no payload */ }
            case ChoiceEvaluated e -> p.put("next", e.next());
            case ChildrenSpawned e -> p.put("count", e.count());
            case ChildSucceeded e -> {
                p.put("index", e.index());
                p.set("output", mapper.valueToTree(e.output()));
            }
            case ChildFailed e -> {
                p.put("index", e.index());
                p.put("error", e.error());
                p.put("cause", e.cause());
            }
            case ExecutionSucceeded e -> p.set("output", mapper.valueToTree(e.output()));
            case ExecutionFailed e -> {
                p.put("error", e.error());
                p.put("cause", e.cause());
            }
        }
        return p;
    }

    /** A payload field as a raw {@link JsonNode} (the engine records carry {@code Object} data). */
    private JsonNode field(JsonNode payload, String name) {
        return payload == null ? null : payload.get(name);
    }

    /** A payload field as a nullable String (missing or JSON null → {@code null}). */
    private String text(JsonNode payload, String name) {
        JsonNode node = field(payload, name);
        return node == null || node.isNull() ? null : node.asText();
    }
}
