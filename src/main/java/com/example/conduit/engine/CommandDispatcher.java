package com.example.conduit.engine;

import com.example.conduit.dispatch.TaskContext;
import com.example.conduit.dispatch.TaskDispatcher;
import com.example.conduit.enums.ExecutionStatus;
import com.example.conduit.enums.TaskStatus;
import com.example.conduit.enums.TaskType;
import com.example.conduit.model.Execution;
import com.example.conduit.model.Task;
import com.example.conduit.repository.ExecutionRepository;
import com.example.conduit.repository.TaskRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Lazy;
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
    private final ExecutionRepository executionRepository;
    private final TaskDispatcher taskDispatcher;
    private final EngineService engineService;
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;

    public CommandDispatcher(TaskRepository taskRepository, ExecutionRepository executionRepository,
                             TaskDispatcher taskDispatcher, @Lazy EngineService engineService,
                             StringRedisTemplate redis, ObjectMapper mapper) {
        this.taskRepository = taskRepository;
        this.executionRepository = executionRepository;
        this.taskDispatcher = taskDispatcher;
        this.engineService = engineService;
        this.redis = redis;
        this.mapper = mapper;
    }

    public void dispatch(String executionId, List<Command> commands) {
        for (Command command : commands) {
            switch (command) {
                case EnqueueTask enqueue -> enqueueTask(executionId, enqueue);
                case ScheduleTimer timer -> scheduleTimer(executionId, timer);
                case SendToDlq dlq -> sendToDlq(executionId, dlq);
                case SpawnChildren spawn -> spawnChildren(executionId, spawn);
                case CompleteExecution ignored -> { /* terminal projection is written in the loop tx */ }
            }
        }
    }

    /**
     * Fans out a Parallel/Map state into child executions: one row per input (recording parent id,
     * branch index, and the parent state so the child resolves its sub-graph), each started through
     * the engine loop. A child already spawned for an index is skipped, so a re-dispatch is a no-op.
     */
    private void spawnChildren(String parentId, SpawnChildren spawn) {
        Execution parent = executionRepository.findById(parentId).orElseThrow();
        List<Object> inputs = spawn.inputs();
        for (int index = 0; index < inputs.size(); index++) {
            String childName = parentId + ":" + spawn.state() + ":" + index;
            if (executionRepository.existsByParentExecutionIdAndBranchStateAndParentBranchIndex(
                    parentId, spawn.state(), index)) {
                continue; // already spawned — effectively-once
            }
            JsonNode childInput = toNode(inputs.get(index));
            Execution child = Execution.builder()
                    .workflowDefinitionId(parent.getWorkflowDefinitionId())
                    .name(childName)
                    .status(ExecutionStatus.RUNNING)
                    .input(childInput)
                    .parentExecutionId(parentId)
                    .parentBranchIndex(index)
                    .branchState(spawn.state())
                    .rootExecutionId(parent.getRootExecutionId())
                    .startedAt(Instant.now())
                    .build();
            executionRepository.save(child);
            engineService.trigger(child.getId(), new ExecutionStarted(childInput));
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
