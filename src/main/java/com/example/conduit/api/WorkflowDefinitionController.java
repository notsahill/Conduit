package com.example.conduit.api;

import com.example.conduit.api.dto.CreateWorkflowDefinitionRequest;
import com.example.conduit.api.dto.CreateWorkflowDefinitionResponse;
import com.example.conduit.api.dto.StartExecutionRequest;
import com.example.conduit.api.dto.StartExecutionResponse;
import com.example.conduit.api.dto.WorkflowDefinitionView;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** CreateWorkflowDefinition / DescribeWorkflowDefinition / ListWorkflowDefinitions + StartExecution. */
@RestController
@RequestMapping("/workflow-definitions")
public class WorkflowDefinitionController {

    private final WorkflowDefinitionService definitionService;
    private final ExecutionService executionService;

    public WorkflowDefinitionController(WorkflowDefinitionService definitionService,
                                        ExecutionService executionService) {
        this.definitionService = definitionService;
        this.executionService = executionService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CreateWorkflowDefinitionResponse create(@RequestBody CreateWorkflowDefinitionRequest request) {
        return definitionService.create(request);
    }

    @GetMapping
    public List<WorkflowDefinitionView> list() {
        return definitionService.list().stream().map(WorkflowDefinitionView::of).toList();
    }

    @GetMapping("/{id}")
    public WorkflowDefinitionView describe(@PathVariable String id) {
        return WorkflowDefinitionView.of(definitionService.get(id));
    }

    @PostMapping("/{id}/executions")
    @ResponseStatus(HttpStatus.CREATED)
    public StartExecutionResponse start(@PathVariable String id, @RequestBody StartExecutionRequest request) {
        return executionService.start(id, request);
    }
}
