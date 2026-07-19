package com.example.conduit.api.dto;

/** Result of registering a definition: its generated id and assigned version. */
public record CreateWorkflowDefinitionResponse(String id, int version) {
}
