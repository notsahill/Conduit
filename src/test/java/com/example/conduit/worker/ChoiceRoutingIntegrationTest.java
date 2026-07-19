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

/** Phase 6 done-when: a branching workflow routes per the task output through a Choice to the right branch. */
@SpringBootTest(properties = "conduit.streams.autostart=false")
@Import(TestcontainersConfiguration.class)
class ChoiceRoutingIntegrationTest {

    @Autowired WorkflowDefinitionService definitionService;
    @Autowired ExecutionService executionService;
    @Autowired WorkerRuntime worker;
    @Autowired ResultConsumer resultConsumer;
    @Autowired ExecutionRepository executionRepository;
    @Autowired ObjectMapper mapper;

    @Test
    void taskOutputRoutesThroughChoiceToInvoiceBranch() throws Exception {
        worker.register("classifier", input -> mapper.createObjectNode().put("docType", "invoice"));

        String machine = """
                { "StartAt": "Classify",
                  "States": {
                    "Classify": { "Type": "Task", "Resource": "classifier", "Next": "Route" },
                    "Route": { "Type": "Choice",
                               "Choices": [{ "Variable": "$.docType", "StringEquals": "invoice", "Next": "Invoice" }],
                               "Default": "Generic" },
                    "Invoice": { "Type": "Pass", "Result": { "handled": "invoice" }, "End": true },
                    "Generic": { "Type": "Pass", "Result": { "handled": "generic" }, "End": true }
                  } }
                """;
        String defId = definitionService.create(
                new CreateWorkflowDefinitionRequest("choice-" + System.nanoTime(), node(machine))).id();
        String execId = executionService.start(defId,
                new StartExecutionRequest(null, node("{}"))).executionId();

        assertThat(worker.poll("classifier", "w")).isEqualTo(1);
        assertThat(resultConsumer.poll("c")).isEqualTo(1);

        Execution exec = executionRepository.findById(execId).orElseThrow();
        assertThat(exec.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        assertThat(exec.getCurrentState()).isEqualTo("Invoice");
        assertThat(exec.getOutput().get("handled").asText()).isEqualTo("invoice");
    }

    private JsonNode node(String json) throws Exception {
        return mapper.readTree(json);
    }
}
