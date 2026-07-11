package com.example.conduit.dsl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * A state in a workflow definition. Jackson dispatches on the {@code "Type"} discriminator to the
 * matching record. Sealed so {@code decide()} can switch exhaustively over the state types.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "Type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = TaskState.class, name = "Task"),
        @JsonSubTypes.Type(value = PassState.class, name = "Pass"),
        @JsonSubTypes.Type(value = ChoiceState.class, name = "Choice"),
        @JsonSubTypes.Type(value = WaitState.class, name = "Wait"),
        @JsonSubTypes.Type(value = SucceedState.class, name = "Succeed"),
        @JsonSubTypes.Type(value = FailState.class, name = "Fail"),
        @JsonSubTypes.Type(value = ParallelState.class, name = "Parallel"),
        @JsonSubTypes.Type(value = MapState.class, name = "Map"),
})
public sealed interface State
        permits TaskState, PassState, ChoiceState, WaitState,
        SucceedState, FailState, ParallelState, MapState {
}
