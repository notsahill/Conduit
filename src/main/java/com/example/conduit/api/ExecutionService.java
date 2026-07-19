package com.example.conduit.api;

import com.example.conduit.api.dto.StartExecutionRequest;
import com.example.conduit.api.dto.StartExecutionResponse;
import com.example.conduit.engine.EngineService;
import com.example.conduit.engine.ExecutionStarted;
import com.example.conduit.enums.ExecutionStatus;
import com.example.conduit.model.Execution;
import com.example.conduit.model.ExecutionEvent;
import com.example.conduit.repository.ExecutionEventRepository;
import com.example.conduit.repository.ExecutionRepository;
import com.example.conduit.repository.WorkflowDefinitionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/** Starts and reads executions. Start inserts the run row, then drives the engine's first trigger. */
@Service
public class ExecutionService {

    private final WorkflowDefinitionRepository definitionRepository;
    private final ExecutionRepository executionRepository;
    private final ExecutionEventRepository eventRepository;
    private final EngineService engineService;
    private final ObjectMapper objectMapper;

    public ExecutionService(WorkflowDefinitionRepository definitionRepository,
                            ExecutionRepository executionRepository,
                            ExecutionEventRepository eventRepository,
                            EngineService engineService,
                            ObjectMapper objectMapper) {
        this.definitionRepository = definitionRepository;
        this.executionRepository = executionRepository;
        this.eventRepository = eventRepository;
        this.engineService = engineService;
        this.objectMapper = objectMapper;
    }

    /**
     * Inserts the RUNNING projection row (committed on its own), then fires the {@code ExecutionStarted}
     * trigger through the engine loop — which appends the events, enters the first state, and dispatches
     * its task. The row is committed first so the loop's {@code SELECT ... FOR UPDATE} can lock it.
     */
    public StartExecutionResponse start(String definitionId, StartExecutionRequest request) {
        if (!definitionRepository.existsById(definitionId)) {
            throw new NotFoundException("workflow definition '" + definitionId + "' not found");
        }
        JsonNode input = request.input() != null ? request.input() : objectMapper.nullNode();

        Execution execution = Execution.builder()
                .workflowDefinitionId(definitionId)
                .status(ExecutionStatus.RUNNING)
                .input(input)
                .startedAt(Instant.now())
                .build();
        String name = request.name() != null ? request.name() : execution.getId();
        execution.setName(name);
        execution.setRootExecutionId(execution.getId());
        executionRepository.save(execution);

        engineService.trigger(execution.getId(), new ExecutionStarted(input));

        return new StartExecutionResponse(execution.getId());
    }

    @Transactional(readOnly = true)
    public Execution get(String id) {
        return executionRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("execution '" + id + "' not found"));
    }

    @Transactional(readOnly = true)
    public List<ExecutionEvent> history(String id) {
        if (!executionRepository.existsById(id)) {
            throw new NotFoundException("execution '" + id + "' not found");
        }
        return eventRepository.findByExecutionIdOrderBySeqAsc(id);
    }
}
