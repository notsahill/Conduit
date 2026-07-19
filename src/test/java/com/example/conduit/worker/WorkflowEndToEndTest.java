package com.example.conduit.worker;

import com.example.conduit.TestcontainersConfiguration;
import com.example.conduit.api.ExecutionService;
import com.example.conduit.api.WorkflowDefinitionService;
import com.example.conduit.api.dto.CreateWorkflowDefinitionRequest;
import com.example.conduit.api.dto.StartExecutionRequest;
import com.example.conduit.enums.ExecutionStatus;
import com.example.conduit.model.Execution;
import com.example.conduit.repository.ExecutionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 4 "done when": a linear Task workflow runs end-to-end across a real worker over real Redis.
 * Start dispatches the task to its stream; the worker runs its handler and reports; the result
 * consumer feeds it back to the engine, which drives the machine to a SUCCEEDED terminal. Pumped
 * deterministically (explicit polls) rather than racing background threads.
 */
@SpringBootTest(properties = "conduit.streams.autostart=false")
@Import(TestcontainersConfiguration.class)
class WorkflowEndToEndTest {

    private static final String MACHINE = """
            { "StartAt": "Ocr",
              "States": {
                "Ocr":  { "Type": "Task", "Resource": "e2e-ocr", "Next": "Done" },
                "Done": { "Type": "Succeed" }
              } }
            """;

    @Autowired WorkflowDefinitionService definitionService;
    @Autowired ExecutionService executionService;
    @Autowired WorkerRuntime worker;
    @Autowired ResultConsumer resultConsumer;
    @Autowired ExecutionRepository executionRepository;
    @Autowired ObjectMapper mapper;

    @Test
    void linearTaskWorkflowRunsAcrossRealWorker() throws Exception {
        worker.register("e2e-ocr", input ->
                mapper.createObjectNode().put("text", input.get("doc").asText().toUpperCase()));

        String defId = definitionService.create(
                new CreateWorkflowDefinitionRequest("e2e-" + System.nanoTime(), node(MACHINE))).id();
        String execId = executionService.start(defId,
                new StartExecutionRequest(null, node("{ \"doc\": \"hello\" }"))).executionId();

        // Task was dispatched to task:e2e-ocr on start — the worker picks it up and reports.
        assertThat(worker.poll("e2e-ocr", "w1")).isEqualTo(1);
        // The engine consumes the result and drives Ocr -> Done -> SUCCEEDED.
        assertThat(resultConsumer.poll("c1")).isEqualTo(1);

        Execution exec = executionRepository.findById(execId).orElseThrow();
        assertThat(exec.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        assertThat(exec.getCurrentState()).isEqualTo("Done");
        assertThat(exec.getOutput().get("text").asText()).isEqualTo("HELLO");
    }

    private JsonNode node(String json) throws Exception {
        return mapper.readTree(json);
    }
}
