package com.example.conduit.dsl;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Wait: park until {@code Seconds} (static) or {@code SecondsPath} (from data) elapse. */
public record WaitState(
        @JsonProperty("Seconds") Integer seconds,
        @JsonProperty("SecondsPath") String secondsPath,
        @JsonProperty("Next") String next,
        @JsonProperty("End") boolean end
) implements State {
}
