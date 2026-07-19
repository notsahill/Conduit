package com.example.conduit.api;

import com.example.conduit.dsl.DslParseException;
import com.example.conduit.dsl.DslValidationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;
import java.util.Map;

/** Maps domain exceptions to HTTP responses for the control-plane API. */
@RestControllerAdvice
public class ApiExceptionHandler {

    /** Definition failed graph validation → 400 with every collected error. */
    @ExceptionHandler(DslValidationException.class)
    public ResponseEntity<Map<String, List<String>>> onValidation(DslValidationException ex) {
        return ResponseEntity.badRequest().body(Map.of("errors", ex.errors()));
    }

    /** Definition JSON was malformed / not a state machine → 400. */
    @ExceptionHandler(DslParseException.class)
    public ResponseEntity<Map<String, List<String>>> onParse(DslParseException ex) {
        return ResponseEntity.badRequest().body(Map.of("errors", List.of(ex.getMessage())));
    }

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<Map<String, String>> onNotFound(NotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", ex.getMessage()));
    }
}
