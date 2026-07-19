package com.example.conduit.engine;

/**
 * A side-effecting instruction produced by {@link Engine#decide}. The engine core stays pure by
 * returning commands rather than performing IO; the dispatcher executes them after the events are
 * committed (Phase 4+). Sealed so the dispatcher handles every case.
 */
public sealed interface Command permits EnqueueTask, CompleteExecution, ScheduleTimer, SendToDlq {
}
