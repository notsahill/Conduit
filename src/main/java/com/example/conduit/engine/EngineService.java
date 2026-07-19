package com.example.conduit.engine;

import com.example.conduit.api.NotFoundException;
import com.example.conduit.dsl.DslParser;
import com.example.conduit.dsl.WorkflowGraph;
import com.example.conduit.enums.ExecutionStatus;
import com.example.conduit.model.Execution;
import com.example.conduit.model.ExecutionEvent;
import com.example.conduit.repository.ExecutionEventRepository;
import com.example.conduit.repository.ExecutionRepository;
import com.example.conduit.repository.WorkflowDefinitionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The engine loop — the single writer per execution. Every trigger runs the same critical section:
 * lock the execution row, replay its durable log, {@code decide()}, append the new events, and update
 * the projection — all in one transaction. Commands dispatch only <em>after</em> commit, so a
 * rolled-back decision emits no side effects (see the README "Concurrency & correctness" gap #1).
 */
@Service
public class EngineService {

    private final ExecutionRepository executionRepository;
    private final ExecutionEventRepository eventRepository;
    private final WorkflowDefinitionRepository definitionRepository;
    private final EngineEventCodec codec;
    private final CommandDispatcher commandDispatcher;
    private final ObjectMapper mapper;
    private final TransactionTemplate txTemplate;

    public EngineService(ExecutionRepository executionRepository,
                         ExecutionEventRepository eventRepository,
                         WorkflowDefinitionRepository definitionRepository,
                         EngineEventCodec codec,
                         CommandDispatcher commandDispatcher,
                         ObjectMapper mapper,
                         PlatformTransactionManager txManager) {
        this.executionRepository = executionRepository;
        this.eventRepository = eventRepository;
        this.definitionRepository = definitionRepository;
        this.codec = codec;
        this.commandDispatcher = commandDispatcher;
        this.mapper = mapper;
        this.txTemplate = new TransactionTemplate(txManager);
    }

    /**
     * Applies one trigger to an execution and dispatches whatever it produces. The append is
     * transactional and serialized per execution; dispatch runs after the commit returns.
     */
    public void trigger(String executionId, EngineEvent triggerEvent) {
        List<Command> commands = txTemplate.execute(status -> append(executionId, triggerEvent));
        commandDispatcher.dispatch(executionId, commands == null ? List.of() : commands);
    }

    private List<Command> append(String executionId, EngineEvent triggerEvent) {
        Execution execution = executionRepository.findByIdForUpdate(executionId)
                .orElseThrow(() -> new NotFoundException("execution '" + executionId + "' not found"));

        List<ExecutionEvent> rows = eventRepository.findByExecutionIdOrderBySeqAsc(executionId);
        List<EngineEvent> log = new ArrayList<>(rows.stream().map(codec::fromRow).toList());

        if (isNoOp(Replay.replay(log), triggerEvent)) {
            return List.of(); // stale/duplicate trigger — idempotent no-op
        }

        // The trigger is itself an event; replay it into the state so decide() sees the data it
        // carries (e.g. an ExecutionStarted's input), matching the pure engine's driver contract.
        log.add(triggerEvent);
        ExecutionState state = Replay.replay(log);
        WorkflowGraph graph = DslParser.parse(definition(execution).toString());
        DecideResult result = Engine.decide(graph, state, triggerEvent);
        log.addAll(result.events());

        int seq = rows.size();
        List<EngineEvent> appended = new ArrayList<>();
        appended.add(triggerEvent);
        appended.addAll(result.events());
        for (EngineEvent event : appended) {
            eventRepository.save(codec.toRow(executionId, seq++, event));
        }

        project(execution, Replay.replay(log), result.commands());
        return result.commands();
    }

    /**
     * A task result / timer whose execution is already terminal, or that targets a state the machine
     * has moved past, is a duplicate or stale redelivery. Applying it would double-advance the log,
     * so it is dropped. (Engine-side state stays exactly-once regardless of handler idempotency.)
     */
    private boolean isNoOp(ExecutionState state, EngineEvent trigger) {
        return switch (trigger) {
            case TaskSucceeded ts -> isTerminal(state) || !ts.state().equals(state.currentStateName());
            case TaskFailed tf -> isTerminal(state) || !tf.state().equals(state.currentStateName());
            default -> false;
        };
    }

    private boolean isTerminal(ExecutionState state) {
        return state.status() != null && state.status() != ExecutionStatus.RUNNING;
    }

    /** Writes the projection cache: status, current state, and (on completion) output/error + stop time. */
    private void project(Execution execution, ExecutionState state, List<Command> commands) {
        execution.setStatus(state.status());
        execution.setCurrentState(state.currentStateName());
        commands.stream()
                .filter(CompleteExecution.class::isInstance)
                .map(CompleteExecution.class::cast)
                .findFirst()
                .ifPresent(complete -> {
                    execution.setOutput(toNode(complete.output()));
                    execution.setError(toNode(complete.error()));
                    execution.setStoppedAt(Instant.now());
                });
        executionRepository.save(execution);
    }

    private com.fasterxml.jackson.databind.JsonNode definition(Execution execution) {
        return definitionRepository.findById(execution.getWorkflowDefinitionId())
                .orElseThrow(() -> new NotFoundException(
                        "workflow definition '" + execution.getWorkflowDefinitionId() + "' not found"))
                .getDefinition();
    }

    private com.fasterxml.jackson.databind.JsonNode toNode(Object value) {
        return value == null ? null : mapper.valueToTree(value);
    }
}
