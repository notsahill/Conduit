package com.example.conduit.worker;

import com.example.conduit.TestcontainersConfiguration;
import com.example.conduit.api.ExecutionService;
import com.example.conduit.api.WorkflowDefinitionService;
import com.example.conduit.api.dto.CreateWorkflowDefinitionRequest;
import com.example.conduit.api.dto.StartExecutionRequest;
import com.example.conduit.enums.ExecutionStatus;
import com.example.conduit.repository.ExecutionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Crash recovery: a worker reads a task then dies before acking; the reaper reclaims the stuck entry
 * and it completes. No duplicate side effect — the reclaim reprocesses the one unacked attempt.
 */
@SpringBootTest(properties = "conduit.streams.autostart=false")
@Import(TestcontainersConfiguration.class)
class WorkerRecoveryTest {

    @Autowired WorkflowDefinitionService definitionService;
    @Autowired ExecutionService executionService;
    @Autowired WorkerRuntime worker;
    @Autowired ResultConsumer resultConsumer;
    @Autowired ExecutionRepository executionRepository;
    @Autowired StringRedisTemplate redis;
    @Autowired ObjectMapper mapper;

    @Test
    void reaperReclaimsTaskFromCrashedWorker() throws Exception {
        String resource = "reap-" + System.nanoTime();
        worker.register(resource, input -> mapper.createObjectNode().put("done", true));

        String machine = """
                { "StartAt": "A",
                  "States": {
                    "A":    { "Type": "Task", "Resource": "%s", "Next": "Done" },
                    "Done": { "Type": "Succeed" }
                  } }
                """.formatted(resource);
        String defId = definitionService.create(
                new CreateWorkflowDefinitionRequest("reap-" + System.nanoTime(), node(machine))).id();
        String execId = executionService.start(defId,
                new StartExecutionRequest(null, node("{}"))).executionId();

        // A worker reads the entry into its PEL then "crashes" — never processes, never acks.
        String stream = "task:" + resource;
        String group = resource + "-workers";
        redis.opsForStream().createGroup(stream, ReadOffset.from("0"), group);
        redis.opsForStream().read(Consumer.from(group, "dead-worker"),
                StreamReadOptions.empty().count(1),
                StreamOffset.create(stream, ReadOffset.lastConsumed()));

        // The reaper reclaims anything idle and reprocesses it.
        assertThat(worker.reclaim(resource, "reaper", 0)).isEqualTo(1);
        assertThat(resultConsumer.poll("c")).isEqualTo(1);

        assertThat(executionRepository.findById(execId).orElseThrow().getStatus())
                .isEqualTo(ExecutionStatus.SUCCEEDED);
    }

    private JsonNode node(String json) throws Exception {
        return mapper.readTree(json);
    }
}
