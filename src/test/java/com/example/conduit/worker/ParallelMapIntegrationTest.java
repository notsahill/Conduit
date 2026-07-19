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

/** Phase 7 done-when: Parallel and Map fan out to child executions and fan in ordered aggregate output. */
@SpringBootTest(properties = "conduit.streams.autostart=false")
@Import(TestcontainersConfiguration.class)
class ParallelMapIntegrationTest {

    @Autowired WorkflowDefinitionService definitionService;
    @Autowired ExecutionService executionService;
    @Autowired WorkerRuntime worker;
    @Autowired ResultConsumer resultConsumer;
    @Autowired ExecutionRepository executionRepository;
    @Autowired ObjectMapper mapper;

    @Test
    void parallelBranchesFanOutOverWorkersAndAggregate() throws Exception {
        worker.register("work", input -> mapper.createObjectNode().put("ok", true));
        String machine = """
                { "StartAt": "Fork",
                  "States": {
                    "Fork": { "Type": "Parallel", "End": true, "Branches": [
                        { "StartAt": "W0", "States": { "W0": { "Type": "Task", "Resource": "work", "End": true } } },
                        { "StartAt": "W1", "States": { "W1": { "Type": "Task", "Resource": "work", "End": true } } }
                    ] }
                  } }
                """;
        String defId = definitionService.create(
                new CreateWorkflowDefinitionRequest("par-" + System.nanoTime(), node(machine))).id();
        String execId = executionService.start(defId, new StartExecutionRequest(null, node("{}"))).executionId();

        // Both branches dispatched a task to the same resource stream; the worker handles both.
        assertThat(worker.poll("work", "w")).isEqualTo(2);
        assertThat(resultConsumer.poll("c")).isEqualTo(2);

        Execution exec = executionRepository.findById(execId).orElseThrow();
        assertThat(exec.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        assertThat(exec.getOutput().isArray()).isTrue();
        assertThat(exec.getOutput()).hasSize(2);
    }

    @Test
    void mapFansOutPerItemAndAggregatesInOrder() throws Exception {
        String machine = """
                { "StartAt": "Each",
                  "States": {
                    "Each": { "Type": "Map", "ItemsPath": "$.items", "End": true,
                              "Iterator": { "StartAt": "I", "States": { "I": { "Type": "Pass", "End": true } } } }
                  } }
                """;
        String defId = definitionService.create(
                new CreateWorkflowDefinitionRequest("map-" + System.nanoTime(), node(machine))).id();

        // Instant iterator (Pass) → children complete synchronously as they are spawned.
        String execId = executionService.start(defId,
                new StartExecutionRequest(null, node("{ \"items\": [ {\"n\":1}, {\"n\":2}, {\"n\":3} ] }"))).executionId();

        Execution exec = executionRepository.findById(execId).orElseThrow();
        assertThat(exec.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        assertThat(exec.getOutput()).hasSize(3);
        assertThat(exec.getOutput().get(0).get("n").asInt()).isEqualTo(1);
        assertThat(exec.getOutput().get(1).get("n").asInt()).isEqualTo(2);
        assertThat(exec.getOutput().get(2).get("n").asInt()).isEqualTo(3);
    }

    private JsonNode node(String json) throws Exception {
        return mapper.readTree(json);
    }
}
