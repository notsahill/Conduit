package com.example.conduit.api;

import com.example.conduit.api.dto.DlqEntryView;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Inspect the dead-letter stream. Redrive is deferred to v2 (requires reopening a failed execution). */
@RestController
@RequestMapping("/dlq")
public class DlqController {

    private final DlqService dlqService;

    public DlqController(DlqService dlqService) {
        this.dlqService = dlqService;
    }

    @GetMapping
    public List<DlqEntryView> list() {
        return dlqService.list();
    }
}
