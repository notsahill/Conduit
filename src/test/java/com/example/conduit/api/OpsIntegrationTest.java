package com.example.conduit.api;

import com.example.conduit.TestcontainersConfiguration;
import com.example.conduit.api.dto.CreateWorkflowDefinitionRequest;
import com.example.conduit.api.dto.StartExecutionRequest;
import com.example.conduit.engine.Scheduler;
import com.example.conduit.enums.ExecutionStatus;
import com.example.conduit.model.Execution;
import com.example.conduit.repository.ExecutionRepository;
import com.example.conduit.worker.ResultConsumer;
import com.example.conduit.worker.WorkerRuntime;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Phase 8 ops surface: list filters, task inspection, StopExecution cascade, DLQ inspection. */
@SpringBootTest(properties = "conduit.streams.autostart=false")
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class OpsIntegrationTest {

    @Autowired MockMvc mvc;
    @Autowired WorkflowDefinitionService definitionService;
    @Autowired ExecutionService executionService;
    @Autowired ExecutionRepository executionRepository;
    @Autowired WorkerRuntime worker;
    @Autowired ResultConsumer resultConsumer;
    @Autowired Scheduler scheduler;
    @Autowired ObjectMapper mapper;

    private static final String TASK_MACHINE = """
            { "StartAt": "T",
              "States": { "T": { "Type": "Task", "Resource": "noop", "Next": "D" }, "D": { "Type": "Succeed" } } }
            """;

    @Test
    void listExecutionsFiltersByDefinitionAndStatus() throws Exception {
        String defId = createDef(TASK_MACHINE);
        String execId = start(defId, "{}");

        mvc.perform(get("/executions")
                        .param("workflowDefinitionId", defId).param("status", "RUNNING"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value(execId))
                .andExpect(jsonPath("$[0].status").value("RUNNING"));
    }

    @Test
    void tasksEndpointListsDispatchedTask() throws Exception {
        String defId = createDef(TASK_MACHINE);
        String execId = start(defId, "{}");

        mvc.perform(get("/executions/{id}/tasks", execId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", org.hamcrest.Matchers.hasSize(1)))
                .andExpect(jsonPath("$[0].stateName").value("T"))
                .andExpect(jsonPath("$[0].resource").value("noop"))
                .andExpect(jsonPath("$[0].attempt").value(1));
    }

    @Test
    void stopExecutionAbortsAndCascadesToChildren() throws Exception {
        String parallel = """
                { "StartAt": "Fork",
                  "States": { "Fork": { "Type": "Parallel", "End": true, "Branches": [
                      { "StartAt": "W0", "States": { "W0": { "Type": "Task", "Resource": "noop", "End": true } } },
                      { "StartAt": "W1", "States": { "W1": { "Type": "Task", "Resource": "noop", "End": true } } }
                  ] } } }
                """;
        String defId = createDef(parallel);
        String execId = start(defId, "{}");
        List<Execution> children = executionRepository.findByParentExecutionId(execId);
        assertThat(children).hasSize(2);

        mvc.perform(post("/executions/{id}/stop", execId)).andExpect(status().isOk());

        assertThat(executionRepository.findById(execId).orElseThrow().getStatus())
                .isEqualTo(ExecutionStatus.ABORTED);
        assertThat(children).allSatisfy(child ->
                assertThat(executionRepository.findById(child.getId()).orElseThrow().getStatus())
                        .isEqualTo(ExecutionStatus.ABORTED));
    }

    @Test
    void dlqEndpointListsDeadLetteredTasks() throws Exception {
        worker.register("flaky", input -> {
            throw new IllegalStateException("always fails");
        });
        String machine = """
                { "StartAt": "A",
                  "States": {
                    "A": { "Type": "Task", "Resource": "flaky",
                           "Retry": [{ "ErrorEquals": ["States.ALL"], "IntervalSeconds": 0, "MaxAttempts": 1 }],
                           "Next": "D" },
                    "D": { "Type": "Succeed" }
                  } }
                """;
        String defId = createDef(machine);
        start(defId, "{}");
        worker.poll("flaky", "w");
        resultConsumer.poll("c"); // attempt 1 == MaxAttempts → exhausted → DLQ

        mvc.perform(get("/dlq"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", org.hamcrest.Matchers.hasSize(greaterThanOrEqualTo(1))))
                .andExpect(jsonPath("$[?(@.stateName == 'A')]", org.hamcrest.Matchers.not(org.hamcrest.Matchers.empty())));
    }

    private String createDef(String machine) throws Exception {
        return definitionService.create(
                new CreateWorkflowDefinitionRequest("ops-" + System.nanoTime(), node(machine))).id();
    }

    private String start(String defId, String input) throws Exception {
        return executionService.start(defId, new StartExecutionRequest(null, node(input))).executionId();
    }

    private JsonNode node(String json) throws Exception {
        return mapper.readTree(json);
    }
}
