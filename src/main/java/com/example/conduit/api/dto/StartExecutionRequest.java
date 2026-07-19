package com.example.conduit.api.dto;

import com.fasterxml.jackson.databind.JsonNode;

/** Request to start an execution: optional caller-supplied name (idempotency) plus the input JSON. */
public record StartExecutionRequest(String name, JsonNode input) {
}
