package com.example.conduit.api.dto;

import com.example.conduit.model.WorkflowDefinition;

import java.time.Instant;

/** Read model for describe/list: the stored definition with its metadata. */
public record WorkflowDefinitionView(
        String id,
        String name,
        int version,
        Object definition,
        Instant createdAt
) {
    public static WorkflowDefinitionView of(WorkflowDefinition def) {
        return new WorkflowDefinitionView(
                def.getId(), def.getName(), def.getVersion(), def.getDefinition(), def.getCreationTime());
    }
}
