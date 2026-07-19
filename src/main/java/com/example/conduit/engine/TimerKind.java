package com.example.conduit.engine;

/**
 * Why a TIMER row exists. All three resume the engine when {@code next_run_at} passes, but produce a
 * different trigger: a Wait completes, a retry re-dispatches, a timeout fails the attempt.
 */
public enum TimerKind {
    WAIT,
    RETRY,
    TIMEOUT
}
