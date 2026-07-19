package com.example.conduit.dispatch;

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

/**
 * Chooses a dispatcher by the resource scheme. v1: every resource is a plain name → Redis stream.
 * The v2 built-in layer (e.g. {@code http:invoke}) slots in here by scheme with no engine change.
 */
@Component
@Primary
public class RoutingTaskDispatcher implements TaskDispatcher {

    private final RedisStreamDispatcher redisDispatcher;

    public RoutingTaskDispatcher(RedisStreamDispatcher redisDispatcher) {
        this.redisDispatcher = redisDispatcher;
    }

    @Override
    public void dispatch(TaskContext ctx) {
        // v1: no schemes yet — a plain resource name maps to its Redis stream.
        redisDispatcher.dispatch(ctx);
    }
}
