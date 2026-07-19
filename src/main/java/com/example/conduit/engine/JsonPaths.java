package com.example.conduit.engine;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Minimal dot-path resolution ({@code $.a.b}) over the JSON data flowing between states. v1 supports
 * only object field navigation — full JSONPath (filters, wildcards, arrays) is out of scope (v2).
 */
final class JsonPaths {

    private JsonPaths() {
    }

    /** Resolves {@code path} against {@code data}, or {@code null} if it is not JSON or does not exist. */
    static JsonNode resolve(Object data, String path) {
        if (!(data instanceof JsonNode node) || path == null) {
            return null;
        }
        String trimmed = path.startsWith("$.") ? path.substring(2)
                : path.startsWith("$") ? path.substring(1) : path;
        for (String segment : trimmed.split("\\.")) {
            if (segment.isEmpty()) {
                continue;
            }
            node = node.get(segment);
            if (node == null) {
                return null;
            }
        }
        return node;
    }
}
