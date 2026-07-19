package com.example.conduit.dispatch;

/**
 * The engine's one seam to the outside world for running tasks. v1 routes everything to Redis
 * Streams; a v2 built-in layer (http:invoke, system:*) can be added behind {@link RoutingTaskDispatcher}
 * without touching the engine.
 */
public interface TaskDispatcher {
    void dispatch(TaskContext ctx);
}
