package com.example.conduit.api;

import com.example.conduit.api.dto.StartExecutionRequest;
import com.example.conduit.api.dto.StartExecutionResponse;
import com.example.conduit.enums.EventType;
import com.example.conduit.enums.ExecutionStatus;
import com.example.conduit.model.Execution;
import com.example.conduit.model.ExecutionEvent;
import com.example.conduit.repository.ExecutionEventRepository;
import com.example.conduit.repository.ExecutionRepository;
import com.example.conduit.repository.WorkflowDefinitionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/** Starts and reads executions. Start persists the projection row and the first event in one tx. */
@Service
public class ExecutionService {

    private final WorkflowDefinitionRepository definitionRepository;
    private final ExecutionRepository executionRepository;
    private final ExecutionEventRepository eventRepository;
    private final ObjectMapper objectMapper;

    public ExecutionService(WorkflowDefinitionRepository definitionRepository,
                            ExecutionRepository executionRepository,
                            ExecutionEventRepository eventRepository,
                            ObjectMapper objectMapper) {
        this.definitionRepository = definitionRepository;
        this.executionRepository = executionRepository;
        this.eventRepository = eventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Records a new run: a RUNNING projection row plus the {@code ExecutionStarted} event at seq 0,
     * atomically. The engine advances the machine on the first trigger (Phase 4); here we only
     * establish the run and its authoritative first event.
     */
    @Transactional
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

        ObjectNode payload = objectMapper.createObjectNode();
        payload.set("input", input);
        ExecutionEvent started = ExecutionEvent.builder()
                .executionId(execution.getId())
                .seq(0)
                .type(EventType.EXECUTION_STARTED)
                .payload(payload)
                .build();
        eventRepository.save(started);

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
