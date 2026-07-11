package com.example.conduit.dsl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

/** Parses a definition JSON string into a typed {@link WorkflowGraph}. Pure; no infra. */
public final class DslParser {

    // Unknown properties are tolerated (e.g. reserved "Parameters") for forward-compat with v2.
    private static final JsonMapper MAPPER = JsonMapper.builder()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .build();

    private DslParser() {
    }

    public static WorkflowGraph parse(String json) {
        try {
            return MAPPER.readValue(json, WorkflowGraph.class);
        } catch (JsonProcessingException e) {
            throw new DslParseException(e.getOriginalMessage(), e);
        }
    }
}
