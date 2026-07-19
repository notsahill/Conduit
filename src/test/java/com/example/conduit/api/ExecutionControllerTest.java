package com.example.conduit.api;

import com.example.conduit.TestcontainersConfiguration;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Phase 3 done-when: start an execution over HTTP; it persists an {@code ExecutionStarted} event plus
 * a RUNNING projection row (one tx), and the ordered history is readable.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class ExecutionControllerTest {

    private static final String VALID_MACHINE = """
            { "StartAt": "Ocr",
              "States": {
                "Ocr":  { "Type": "Task", "Resource": "ocr-handler", "Next": "Done" },
                "Done": { "Type": "Succeed" }
              } }
            """;

    @Autowired
    private MockMvc mvc;

    @Test
    void startReturnsExecutionId() throws Exception {
        String defId = createDefinition();

        mvc.perform(post("/workflow-definitions/{id}/executions", defId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{ \"input\": { \"doc\": \"invoice.pdf\" } }"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.executionId").isNotEmpty());
    }

    @Test
    void startMissingDefinitionReturns404() throws Exception {
        mvc.perform(post("/workflow-definitions/{id}/executions", "no-such-def")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{ \"input\": {} }"))
                .andExpect(status().isNotFound());
    }

    @Test
    void describeReturnsRunningExecution() throws Exception {
        String defId = createDefinition();
        String execId = startExecution(defId, "{ \"doc\": \"a.pdf\" }");

        mvc.perform(get("/executions/{id}", execId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(execId))
                .andExpect(jsonPath("$.workflowDefinitionId").value(defId))
                .andExpect(jsonPath("$.status").value("RUNNING"))
                .andExpect(jsonPath("$.input.doc").value("a.pdf"));
    }

    @Test
    void historyReturnsExecutionStartedEvent() throws Exception {
        String defId = createDefinition();
        String execId = startExecution(defId, "{ \"doc\": \"b.pdf\" }");

        mvc.perform(get("/executions/{id}/history", execId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].seq").value(0))
                .andExpect(jsonPath("$[0].type").value("EXECUTION_STARTED"));
    }

    @Test
    void describeMissingExecutionReturns404() throws Exception {
        mvc.perform(get("/executions/{id}", "no-such-exec"))
                .andExpect(status().isNotFound());
    }

    private String createDefinition() throws Exception {
        String json = mvc.perform(post("/workflow-definitions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{ \"name\": \"exec-def-" + System.nanoTime()
                                + "\", \"definition\": " + VALID_MACHINE + " }"))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return JsonPath.read(json, "$.id");
    }

    private String startExecution(String defId, String input) throws Exception {
        String json = mvc.perform(post("/workflow-definitions/{id}/executions", defId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{ \"input\": " + input + " }"))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return JsonPath.read(json, "$.executionId");
    }
}
