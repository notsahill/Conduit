package com.example.conduit.dsl;

import java.util.List;

/** Thrown when a parsed definition violates one or more structural rules; carries every error. */
public class DslValidationException extends RuntimeException {

    private final transient List<String> errors;

    public DslValidationException(List<String> errors) {
        super(String.join("; ", errors));
        this.errors = List.copyOf(errors);
    }

    public List<String> errors() {
        return errors;
    }
}
