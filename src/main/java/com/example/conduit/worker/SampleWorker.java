package com.example.conduit.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

/**
 * Demo handlers so a freshly started app can run the example workflows end-to-end. Real deployments
 * register their own handlers; these are stubs (uppercase the text, echo the input) that show the
 * protocol working without external systems.
 */
@Component
public class SampleWorker {

    private final WorkerRuntime worker;
    private final ObjectMapper mapper;

    public SampleWorker(WorkerRuntime worker, ObjectMapper mapper) {
        this.worker = worker;
        this.mapper = mapper;
    }

    @PostConstruct
    void registerHandlers() {
        worker.register("ocr-handler", input -> {
            String text = input != null && input.hasNonNull("doc") ? input.get("doc").asText() : "";
            return mapper.createObjectNode().put("text", text.toUpperCase());
        });
        worker.register("echo-handler", input -> input);
    }
}
