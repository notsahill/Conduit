package com.example.conduit.engine;

import java.util.Map;

/**
 * Fan-in bookkeeping for one Parallel/Map state: how many children were spawned, the outputs received
 * so far keyed by index, and whether any child failed. Derived by {@link Replay}; read by the engine
 * to decide when all children are in (aggregate) or one failed (Catch/fail).
 */
public record ChildProgress(int total, Map<Integer, Object> outputs, boolean failed, String error, String cause) {

    public boolean allSucceeded() {
        return !failed && outputs.size() == total;
    }
}
