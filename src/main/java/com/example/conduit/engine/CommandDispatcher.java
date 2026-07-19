package com.example.conduit.engine;

import com.example.conduit.dispatch.TaskContext;
import com.example.conduit.dispatch.TaskDispatcher;
import com.example.conduit.enums.TaskStatus;
import com.example.conduit.enums.TaskType;
import com.example.conduit.model.Task;
import com.example.conduit.repository.TaskRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Executes the {@link Command}s a {@code decide()} produced. Runs <em>after</em> the event append has
 * committed, so a rolled-back decision emits no side effects. Writing the {@code tasks} row is guarded
 * by the per-attempt idempotency key, so a re-dispatch (crash between commit and dispatch) is a no-op.
 */
@Component
public class CommandDispatcher {

    private final TaskRepository taskRepository;
    private final TaskDispatcher taskDispatcher;
    private final ObjectMapper mapper;

    public CommandDispatcher(TaskRepository taskRepository, TaskDispatcher taskDispatcher, ObjectMapper mapper) {
        this.taskRepository = taskRepository;
        this.taskDispatcher = taskDispatcher;
        this.mapper = mapper;
    }

    public void dispatch(String executionId, List<Command> commands) {
        for (Command command : commands) {
            switch (command) {
                case EnqueueTask enqueue -> enqueueTask(executionId, enqueue);
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

    private JsonNode toNode(Object value) {
        return value == null ? null : mapper.valueToTree(value);
    }
}
