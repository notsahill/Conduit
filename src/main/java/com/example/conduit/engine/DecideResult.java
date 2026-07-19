package com.example.conduit.engine;

import java.util.List;

/** The output of a pure {@link Engine#decide} call: events to append and commands to dispatch. */
public record DecideResult(List<EngineEvent> events, List<Command> commands) {
}
