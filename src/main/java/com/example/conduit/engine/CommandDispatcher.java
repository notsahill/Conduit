package com.example.conduit.engine;

import com.example.conduit.dispatch.TaskContext;
import com.example.conduit.dispatch.TaskDispatcher;
import com.example.conduit.enums.TaskStatus;
import com.example.conduit.enums.TaskType;
import com.example.conduit.model.Task;
import com.example.conduit.repository.TaskRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes the {@link Command}s a {@code decide()} produced. Runs <em>after</em> the event append has
 * committed, so a rolled-back decision emits no side effects. Task and timer rows are guarded by
 * per-attempt idempotency keys, so a re-dispatch (crash between commit and dispatch) is a no-op.
 */
@Component
public class CommandDispatcher {

    static final String DLQ_STREAM = "dlq";

    private final TaskRepository taskRepository;
    private final TaskDispatcher taskDispatcher;
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;

    public CommandDispatcher(TaskRepository taskRepository, TaskDispatcher taskDispatcher,
                             StringRedisTemplate redis, ObjectMapper mapper) {
        this.taskRepository = taskRepository;
        this.taskDispatcher = taskDispatcher;
        this.redis = redis;
        this.mapper = mapper;
    }

    public void dispatch(String executionId, List<Command> commands) {
        for (Command command : commands) {
            switch (command) {
                case EnqueueTask enqueue -> enqueueTask(executionId, enqueue);
                case ScheduleTimer timer -> scheduleTimer(executionId, timer);
                case SendToDlq dlq -> sendToDlq(executionId, dlq);
                case CompleteExecution ignored -> { /* terminal projection is written in the loop tx */ }
            }
        }
    }

    private void enqueueTask(String executionId, EnqueueTask enqueue) {
        String idempotencyKey = executionId + ":" + enqueue.state() + ":" + enqueue.attempt();
        if (taskRepository.findByIdempotencyKey(idempotencyKey).isPresent()) {
            return; // already dispatched for this attempt — effectively-once
        }
        Task task = Task.builder()
                .executionId(executionId)
                .stateName(enqueue.state())
                .type(TaskType.TASK)
                .status(TaskStatus.QUEUED)
                .idempotencyKey(idempotencyKey)
                .attempt(enqueue.attempt())
                .resource(enqueue.resource())
                .input(toNode(enqueue.input()))
                .parameters(toNode(enqueue.parameters()))
                .build();
        taskRepository.save(task);

        taskDispatcher.dispatch(new TaskContext(
                task.getId(), executionId, enqueue.state(), enqueue.resource(), enqueue.attempt(),
                idempotencyKey, task.getInput(), task.getParameters()));
    }

    /** Persists a SCHEDULED timer row for the poller to claim once {@code next_run_at} passes. */
    private void scheduleTimer(String executionId, ScheduleTimer timer) {
        String idempotencyKey = executionId + ":" + timer.state() + ":" + timer.kind() + ":" + timer.attempt();
        if (taskRepository.findByIdempotencyKey(idempotencyKey).isPresent()) {
            return; // already scheduled — effectively-once
        }
        Task row = Task.builder()
                .executionId(executionId)
                .stateName(timer.state())
                .type(TaskType.TIMER)
                .status(TaskStatus.SCHEDULED)
                .timerKind(timer.kind().name())
                .idempotencyKey(idempotencyKey)
                .attempt(timer.attempt())
                .nextRunAt(Instant.now().plusSeconds(timer.delaySeconds()))
                .build();
        taskRepository.save(row);
    }

    private void sendToDlq(String executionId, SendToDlq dlq) {
        Map<String, String> fields = new HashMap<>();
        fields.put("executionId", executionId);
        fields.put("stateName", dlq.state());
        fields.put("attempt", Integer.toString(dlq.attempt()));
        fields.put("error", dlq.error() == null ? "" : dlq.error());
        fields.put("cause", dlq.cause() == null ? "" : dlq.cause());
        JsonNode input = toNode(dlq.input());
        fields.put("input", input == null ? "null" : input.toString());
        redis.opsForStream().add(StreamRecords.mapBacked(fields).withStreamKey(DLQ_STREAM));
    }

    private JsonNode toNode(Object value) {
        return value == null ? null : mapper.valueToTree(value);
    }
}
