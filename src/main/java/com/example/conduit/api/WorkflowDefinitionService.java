package com.example.conduit.api;

import com.example.conduit.api.dto.CreateWorkflowDefinitionRequest;
import com.example.conduit.api.dto.CreateWorkflowDefinitionResponse;
import com.example.conduit.dsl.DslParser;
import com.example.conduit.dsl.WorkflowGraph;
import com.example.conduit.dsl.WorkflowGraphValidator;
import com.example.conduit.model.WorkflowDefinition;
import com.example.conduit.repository.WorkflowDefinitionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/** Registers and reads workflow definitions. Validates the DSL graph at create time. */
@Service
public class WorkflowDefinitionService {

    private final WorkflowDefinitionRepository repository;
    private final WorkflowGraphValidator validator = new WorkflowGraphValidator();

    public WorkflowDefinitionService(WorkflowDefinitionRepository repository) {
        this.repository = repository;
    }

    /**
     * Parses and validates the definition, then persists it under the next version for its name.
     * A validation failure throws {@link com.example.conduit.dsl.DslValidationException} → HTTP 400.
     */
    @Transactional
    public CreateWorkflowDefinitionResponse create(CreateWorkflowDefinitionRequest request) {
        WorkflowGraph graph = DslParser.parse(request.definition().toString());
        validator.validate(graph);

        int version = repository.findFirstByNameOrderByVersionDesc(request.name())
                .map(existing -> existing.getVersion() + 1)
                .orElse(1);

        WorkflowDefinition def = WorkflowDefinition.builder()
                .name(request.name())
                .version(version)
                .definition(request.definition())
                .build();
        repository.save(def);
        return new CreateWorkflowDefinitionResponse(def.getId(), version);
    }

    @Transactional(readOnly = true)
    public WorkflowDefinition get(String id) {
        return repository.findById(id)
                .orElseThrow(() -> new NotFoundException("workflow definition '" + id + "' not found"));
    }

    @Transactional(readOnly = true)
    public List<WorkflowDefinition> list() {
        return repository.findAll();
    }
}
