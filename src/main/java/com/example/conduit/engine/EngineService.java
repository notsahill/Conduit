package com.example.conduit.engine;

import com.example.conduit.api.NotFoundException;
import com.example.conduit.dsl.DslParser;
import com.example.conduit.dsl.MapState;
import com.example.conduit.dsl.ParallelState;
import com.example.conduit.dsl.State;
import com.example.conduit.dsl.WorkflowGraph;
import com.example.conduit.enums.ExecutionStatus;
import com.example.conduit.model.Execution;
import com.example.conduit.model.ExecutionEvent;
import com.example.conduit.repository.ExecutionEventRepository;
import com.example.conduit.repository.ExecutionRepository;
import com.example.conduit.repository.WorkflowDefinitionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The engine loop — the single writer per execution. Every trigger runs the same critical section:
 * lock the execution row, replay its durable log, {@code decide()}, append the new events, and update
 * the projection — all in one transaction. Commands dispatch only <em>after</em> commit, so a
 * rolled-back decision emits no side effects (see the README "Concurrency & correctness" gap #1).
 * When a child execution reaches a terminal, its parent is notified after commit (fan-in).
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
     * transactional and serialized per execution; dispatch (and any parent fan-in) runs after commit.
     */
    public void trigger(String executionId, EngineEvent triggerEvent) {
        AppendResult result = txTemplate.execute(status -> append(executionId, triggerEvent));
        if (result == null) {
            return;
        }
        commandDispatcher.dispatch(executionId, result.commands());
        if (result.parentEvent() != null) {
            trigger(result.parentId(), result.parentEvent());
        }
    }

    /**
     * StopExecution: aborts a running execution and cascades to its running children. Each abort runs
     * under the per-execution lock and appends an {@code ExecutionAborted} event, so the stop is
     * durable and replay-consistent. Already-terminal executions are left untouched.
     */
    public void stop(String executionId) {
        List<String> runningChildren = txTemplate.execute(status -> abort(executionId));
        if (runningChildren != null) {
            for (String childId : runningChildren) {
                stop(childId);
            }
        }
    }

    private List<String> abort(String executionId) {
        Execution execution = executionRepository.findByIdForUpdate(executionId)
                .orElseThrow(() -> new NotFoundException("execution '" + executionId + "' not found"));

        List<ExecutionEvent> rows = eventRepository.findByExecutionIdOrderBySeqAsc(executionId);
        List<EngineEvent> log = new ArrayList<>(rows.stream().map(codec::fromRow).toList());
        if (isTerminal(Replay.replay(log))) {
            return List.of(); // already SUCCEEDED/FAILED/ABORTED — nothing to stop or cascade
        }

        EngineEvent aborted = new ExecutionAborted("stopped by operator");
        log.add(aborted);
        eventRepository.save(codec.toRow(executionId, rows.size(), aborted));
        project(execution, Replay.replay(log), List.of());
        execution.setStoppedAt(Instant.now());
        executionRepository.save(execution);

        return executionRepository.findByParentExecutionId(executionId).stream()
                .filter(child -> child.getStatus() == ExecutionStatus.RUNNING)
                .map(Execution::getId)
                .toList();
    }

    private AppendResult append(String executionId, EngineEvent triggerEvent) {
        Execution execution = executionRepository.findByIdForUpdate(executionId)
                .orElseThrow(() -> new NotFoundException("execution '" + executionId + "' not found"));

        List<ExecutionEvent> rows = eventRepository.findByExecutionIdOrderBySeqAsc(executionId);
        List<EngineEvent> log = new ArrayList<>(rows.stream().map(codec::fromRow).toList());

        if (isNoOp(Replay.replay(log), triggerEvent)) {
            return AppendResult.EMPTY; // stale/duplicate trigger — idempotent no-op
        }

        // The trigger is itself an event; replay it into the state so decide() sees the data it
        // carries (e.g. an ExecutionStarted's input), matching the pure engine's driver contract.
        log.add(triggerEvent);
        ExecutionState state = Replay.replay(log);
        WorkflowGraph graph = graphFor(execution);
        DecideResult result = Engine.decide(graph, state, triggerEvent);
        log.addAll(result.events());

        int seq = rows.size();
        List<EngineEvent> appended = new ArrayList<>();
        appended.add(triggerEvent);
        appended.addAll(result.events());
        for (EngineEvent event : appended) {
            eventRepository.save(codec.toRow(executionId, seq++, event));
        }

        ExecutionState finalState = Replay.replay(log);
        project(execution, finalState, result.commands());
        return new AppendResult(result.commands(), parentNotification(execution, finalState, result.commands()));
    }

    /** If this execution just reached a terminal and has a parent, the fan-in trigger to raise on it. */
    private ParentNotification parentNotification(Execution execution, ExecutionState finalState,
                                                  List<Command> commands) {
        if (execution.getParentExecutionId() == null || !isTerminal(finalState)) {
            return null;
        }
        int index = execution.getParentBranchIndex();
        String branchState = execution.getBranchState();
        EngineEvent event;
        if (finalState.status() == ExecutionStatus.SUCCEEDED) {
            event = new ChildSucceeded(branchState, index, finalState.currentData());
        } else {
            String[] errorCause = errorCauseOf(commands);
            event = new ChildFailed(branchState, index, errorCause[0], errorCause[1]);
        }
        return new ParentNotification(execution.getParentExecutionId(), event);
    }

    private String[] errorCauseOf(List<Command> commands) {
        return commands.stream()
                .filter(CompleteExecution.class::isInstance)
                .map(c -> ((CompleteExecution) c).error())
                .filter(Map.class::isInstance)
                .map(Map.class::cast)
                .findFirst()
                .map(m -> new String[]{str(m.get("Error")), str(m.get("Cause"))})
                .orElse(new String[]{"States.ChildFailed", null});
    }

    private String str(Object o) {
        return o == null ? null : o.toString();
    }

    /**
     * A trigger whose execution is already terminal, that targets a state the machine has moved past,
     * or that belongs to a superseded attempt/duplicate child is a stale redelivery. Dropping it keeps
     * engine-side state exactly-once regardless of handler idempotency.
     */
    private boolean isNoOp(ExecutionState state, EngineEvent trigger) {
        return switch (trigger) {
            case TaskSucceeded ts -> staleTaskEvent(state, ts.state(), ts.attempt());
            case TaskFailed tf -> staleTaskEvent(state, tf.state(), tf.attempt());
            case TaskTimedOut tt -> staleTaskEvent(state, tt.state(), tt.attempt());
            case WaitCompleted wc -> isTerminal(state) || !wc.state().equals(state.currentStateName());
            case RetryDue rd -> isTerminal(state) || !rd.state().equals(state.currentStateName());
            case ChildSucceeded cs -> isTerminal(state) || !cs.state().equals(state.currentStateName())
                    || childAlreadyRecorded(state, cs.state(), cs.index());
            case ChildFailed cf -> isTerminal(state) || !cf.state().equals(state.currentStateName());
            default -> false;
        };
    }

    private boolean staleTaskEvent(ExecutionState state, String stateName, int attempt) {
        return isTerminal(state)
                || !stateName.equals(state.currentStateName())
                || attempt != state.attemptOf(stateName);
    }

    private boolean childAlreadyRecorded(ExecutionState state, String stateName, int index) {
        ChildProgress progress = state.childProgressOf(stateName);
        return progress != null && progress.outputs().containsKey(index);
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

    /**
     * The graph an execution runs. A root execution runs its definition; a child execution runs the
     * sub-graph of its parent's Parallel/Map state (identified by {@code branch_state} + branch index).
     */
    private WorkflowGraph graphFor(Execution execution) {
        WorkflowGraph graph = DslParser.parse(definition(execution).toString());
        if (execution.getBranchState() == null) {
            return graph;
        }
        State branch = graph.states().get(execution.getBranchState());
        return switch (branch) {
            case ParallelState parallel -> parallel.branches().get(execution.getParentBranchIndex());
            case MapState map -> map.iterator();
            case null, default -> throw new IllegalStateException(
                    "branch_state '" + execution.getBranchState() + "' is not a Parallel/Map state");
        };
    }

    private JsonNode definition(Execution execution) {
        return definitionRepository.findById(execution.getWorkflowDefinitionId())
                .orElseThrow(() -> new NotFoundException(
                        "workflow definition '" + execution.getWorkflowDefinitionId() + "' not found"))
                .getDefinition();
    }

    private JsonNode toNode(Object value) {
        return value == null ? null : mapper.valueToTree(value);
    }

    private record AppendResult(List<Command> commands, ParentNotification notification) {
        static final AppendResult EMPTY = new AppendResult(List.of(), null);

        String parentId() {
            return notification == null ? null : notification.parentId();
        }

        EngineEvent parentEvent() {
            return notification == null ? null : notification.event();
        }
    }

    private record ParentNotification(String parentId, EngineEvent event) {
    }
}
