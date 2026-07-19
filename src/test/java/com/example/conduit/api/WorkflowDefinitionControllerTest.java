package com.example.conduit.api;

import com.example.conduit.TestcontainersConfiguration;
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
 * Phase 3 control-plane: create a definition (DSL-validated, versioned), describe it, list them.
 * Backed by a real Postgres via Testcontainers.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class WorkflowDefinitionControllerTest {

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
    void createReturnsIdAndFirstVersion() throws Exception {
        mvc.perform(post("/workflow-definitions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("ingest-" + unique(), VALID_MACHINE)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").isNotEmpty())
                .andExpect(jsonPath("$.version").value(1));
    }

    @Test
    void createSecondSameNameIncrementsVersion() throws Exception {
        String name = "billing-" + unique();
        mvc.perform(post("/workflow-definitions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(name, VALID_MACHINE)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.version").value(1));

        mvc.perform(post("/workflow-definitions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(name, VALID_MACHINE)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.version").value(2));
    }

    @Test
    void createInvalidDslReturns400WithErrors() throws Exception {
        String badMachine = """
                { "StartAt": "Nope", "States": { "Done": { "Type": "Succeed" } } }
                """;
        mvc.perform(post("/workflow-definitions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("broken-" + unique(), badMachine)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors", hasSize(1)));
    }

    @Test
    void describeReturnsStoredDefinition() throws Exception {
        String name = "describe-" + unique();
        String id = createAndGetId(name);

        mvc.perform(get("/workflow-definitions/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.name").value(name))
                .andExpect(jsonPath("$.version").value(1))
                .andExpect(jsonPath("$.definition.StartAt").value("Ocr"));
    }

    @Test
    void describeMissingReturns404() throws Exception {
        mvc.perform(get("/workflow-definitions/{id}", "does-not-exist"))
                .andExpect(status().isNotFound());
    }

    @Test
    void listIncludesCreatedDefinition() throws Exception {
        String name = "listed-" + unique();
        createAndGetId(name);

        mvc.perform(get("/workflow-definitions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(org.hamcrest.Matchers.greaterThanOrEqualTo(1))));
    }

    private String createAndGetId(String name) throws Exception {
        String json = mvc.perform(post("/workflow-definitions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(name, VALID_MACHINE)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return com.jayway.jsonpath.JsonPath.read(json, "$.id");
    }

    private static String body(String name, String machineJson) {
        return "{ \"name\": \"" + name + "\", \"definition\": " + machineJson + " }";
    }

    private static String unique() {
        return Long.toString(System.nanoTime());
    }
}
