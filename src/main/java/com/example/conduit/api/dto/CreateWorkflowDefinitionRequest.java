package com.example.conduit.api.dto;

import com.fasterxml.jackson.databind.JsonNode;

/** Request to register a workflow definition: a name plus the raw state-machine JSON. */
public record CreateWorkflowDefinitionRequest(String name, JsonNode definition) {
}
