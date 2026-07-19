package com.example.conduit.api;

/** A requested resource (definition, execution) does not exist → HTTP 404. */
public class NotFoundException extends RuntimeException {
    public NotFoundException(String message) {
        super(message);
    }
}
