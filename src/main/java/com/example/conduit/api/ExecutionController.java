package com.example.conduit.api;

import com.example.conduit.api.dto.ExecutionView;
import com.example.conduit.api.dto.HistoryEntryView;
import com.example.conduit.api.dto.TaskView;
import com.example.conduit.enums.ExecutionStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** DescribeExecution, GetExecutionHistory, ListExecutions, StopExecution, task inspection. */
@RestController
@RequestMapping("/executions")
public class ExecutionController {

    private final ExecutionService executionService;

    public ExecutionController(ExecutionService executionService) {
        this.executionService = executionService;
    }

    @GetMapping
    public List<ExecutionView> list(@RequestParam(required = false) String workflowDefinitionId,
                                    @RequestParam(required = false) ExecutionStatus status) {
        return executionService.list(workflowDefinitionId, status).stream().map(ExecutionView::of).toList();
    }

    @GetMapping("/{id}")
    public ExecutionView describe(@PathVariable String id) {
        return ExecutionView.of(executionService.get(id));
    }

    @GetMapping("/{id}/history")
    public List<HistoryEntryView> history(@PathVariable String id) {
        return executionService.history(id).stream().map(HistoryEntryView::of).toList();
    }

    @GetMapping("/{id}/tasks")
    public List<TaskView> tasks(@PathVariable String id) {
        return executionService.tasks(id).stream().map(TaskView::of).toList();
    }

    @PostMapping("/{id}/stop")
    public ExecutionView stop(@PathVariable String id) {
        executionService.stop(id);
        return ExecutionView.of(executionService.get(id));
    }
}
