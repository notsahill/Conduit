package com.example.conduit.engine;

/**
 * Persist a TIMER row that fires after {@code delaySeconds}. The dispatcher (which owns the clock)
 * computes {@code next_run_at = now + delaySeconds}; the scheduler later claims it and triggers the
 * engine per {@link TimerKind}. {@code attempt} scopes retry/timeout timers to a specific try.
 */
public record ScheduleTimer(String state, TimerKind kind, int delaySeconds, int attempt) implements Command {
}
