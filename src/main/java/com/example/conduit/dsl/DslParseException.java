package com.example.conduit.dsl;

/** Thrown when a definition is not well-formed JSON or references an unknown state {@code Type}. */
public class DslParseException extends RuntimeException {
    public DslParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
