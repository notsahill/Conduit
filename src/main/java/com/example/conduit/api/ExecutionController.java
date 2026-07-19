package com.example.conduit.api;

import com.example.conduit.api.dto.ExecutionView;
import com.example.conduit.api.dto.HistoryEntryView;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** DescribeExecution + GetExecutionHistory (the ordered audit trail). */
@RestController
@RequestMapping("/executions")
public class ExecutionController {

    private final ExecutionService executionService;

    public ExecutionController(ExecutionService executionService) {
        this.executionService = executionService;
    }

    @GetMapping("/{id}")
    public ExecutionView describe(@PathVariable String id) {
        return ExecutionView.of(executionService.get(id));
    }

    @GetMapping("/{id}/history")
    public List<HistoryEntryView> history(@PathVariable String id) {
        return executionService.history(id).stream().map(HistoryEntryView::of).toList();
    }
}
