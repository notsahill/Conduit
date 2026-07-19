package com.example.conduit.engine;

import com.example.conduit.TestcontainersConfiguration;
import com.example.conduit.api.ExecutionService;
import com.example.conduit.api.WorkflowDefinitionService;
import com.example.conduit.api.dto.CreateWorkflowDefinitionRequest;
import com.example.conduit.api.dto.StartExecutionRequest;
import com.example.conduit.enums.ExecutionStatus;
import com.example.conduit.model.Execution;
import com.example.conduit.repository.ExecutionRepository;
import com.example.conduit.worker.ResultConsumer;
import com.example.conduit.worker.WorkerRuntime;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 5 durability over real infra: the scheduler claims due timers ({@code SKIP LOCKED}) to resume
 * a Wait, and a task that keeps failing exhausts its retries and dead-letters while the execution fails.
 */
@SpringBootTest(properties = "conduit.streams.autostart=false")
@Import(TestcontainersConfiguration.class)
class DurabilityIntegrationTest {

    @Autowired WorkflowDefinitionService definitionService;
    @Autowired ExecutionService executionService;
    @Autowired WorkerRuntime worker;
    @Autowired ResultConsumer resultConsumer;
    @Autowired Scheduler scheduler;
    @Autowired ExecutionRepository executionRepository;
    @Autowired StringRedisTemplate redis;
    @Autowired ObjectMapper mapper;

    @Test
    void schedulerResumesAWaitState() throws Exception {
        String machine = """
                { "StartAt": "Hold",
                  "States": {
                    "Hold": { "Type": "Wait", "Seconds": 0, "Next": "Done" },
                    "Done": { "Type": "Succeed" }
                  } }
                """;
        String defId = definitionService.create(
                new CreateWorkflowDefinitionRequest("wait-" + System.nanoTime(), node(machine))).id();
        String execId = executionService.start(defId,
                new StartExecutionRequest(null, node("{ \"n\": 1 }"))).executionId();

        // Parked at the Wait with a due TIMER row; the scheduler claims it and resumes to completion.
        assertThat(executionRepository.findById(execId).orElseThrow().getStatus())
                .isEqualTo(ExecutionStatus.RUNNING);
        assertThat(scheduler.pollOnce()).isGreaterThanOrEqualTo(1);

        assertThat(executionRepository.findById(execId).orElseThrow().getStatus())
                .isEqualTo(ExecutionStatus.SUCCEEDED);
    }

    @Test
    void exhaustedRetriesFailExecutionAndDeadLetter() throws Exception {
        worker.register("flaky", input -> {
            throw new IllegalStateException("always fails");
        });
        String machine = """
                { "StartAt": "A",
                  "States": {
                    "A": { "Type": "Task", "Resource": "flaky",
                           "Retry": [{ "ErrorEquals": ["States.ALL"], "IntervalSeconds": 0, "MaxAttempts": 2 }],
                           "Next": "Done" },
                    "Done": { "Type": "Succeed" }
                  } }
                """;
        String defId = definitionService.create(
                new CreateWorkflowDefinitionRequest("retry-" + System.nanoTime(), node(machine))).id();
        long dlqBefore = dlqSize();

        String execId = executionService.start(defId,
                new StartExecutionRequest(null, node("{}"))).executionId();

        // Attempt 1 fails → retry scheduled (0s) → scheduler re-dispatches attempt 2 → also fails → exhausted.
        assertThat(worker.poll("flaky", "w")).isEqualTo(1);
        assertThat(resultConsumer.poll("c")).isEqualTo(1);
        assertThat(scheduler.pollOnce()).isGreaterThanOrEqualTo(1);
        assertThat(worker.poll("flaky", "w")).isEqualTo(1);
        assertThat(resultConsumer.poll("c")).isEqualTo(1);

        Execution exec = executionRepository.findById(execId).orElseThrow();
        assertThat(exec.getStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(dlqSize()).isGreaterThan(dlqBefore);
    }

    private long dlqSize() {
        Long size = redis.opsForStream().size("dlq");
        return size == null ? 0 : size;
    }

    private JsonNode node(String json) throws Exception {
        return mapper.readTree(json);
    }
}
