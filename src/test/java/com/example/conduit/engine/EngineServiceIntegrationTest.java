package com.example.conduit.engine;

import com.example.conduit.TestcontainersConfiguration;
import com.example.conduit.enums.EventType;
import com.example.conduit.enums.ExecutionStatus;
import com.example.conduit.enums.TaskStatus;
import com.example.conduit.model.Execution;
import com.example.conduit.model.ExecutionEvent;
import com.example.conduit.model.Task;
import com.example.conduit.model.WorkflowDefinition;
import com.example.conduit.repository.ExecutionEventRepository;
import com.example.conduit.repository.ExecutionRepository;
import com.example.conduit.repository.TaskRepository;
import com.example.conduit.repository.WorkflowDefinitionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Phase 4 engine loop end-to-end at the service level: a trigger runs replay → decide → append →
 * project under a per-execution lock, then dispatches commands after commit. Verifies durable events,
 * the projection cache, the {@code tasks} bookkeeping row, and the Redis stream entry.
 */
@SpringBootTest(properties = "conduit.streams.autostart=false")
@Import(TestcontainersConfiguration.class)
class EngineServiceIntegrationTest {

    private static final String MACHINE = """
            { "StartAt": "Ocr",
              "States": {
                "Ocr":  { "Type": "Task", "Resource": "ocr-handler", "Next": "Done" },
                "Done": { "Type": "Succeed" }
              } }
            """;

    @Autowired EngineService engineService;
    @Autowired WorkflowDefinitionRepository definitionRepository;
    @Autowired ExecutionRepository executionRepository;
    @Autowired ExecutionEventRepository eventRepository;
    @Autowired TaskRepository taskRepository;
    @Autowired StringRedisTemplate redis;
    @Autowired ObjectMapper mapper;

    @Test
    void firstTriggerEntersTaskAppendsEventsDispatchesAndProjects() throws Exception {
        String execId = seedRunningExecution();

        engineService.trigger(execId, new ExecutionStarted(new TextNode("scan.pdf")));

        List<ExecutionEvent> log = eventRepository.findByExecutionIdOrderBySeqAsc(execId);
        assertThat(log).extracting(ExecutionEvent::getType).containsExactly(
                EventType.EXECUTION_STARTED, EventType.STATE_ENTERED, EventType.TASK_SCHEDULED);
        assertThat(log).extracting(ExecutionEvent::getSeq).containsExactly(0, 1, 2);

        Execution exec = executionRepository.findById(execId).orElseThrow();
        assertThat(exec.getStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(exec.getCurrentState()).isEqualTo("Ocr");

        List<Task> tasks = taskRepository.findByExecutionId(execId);
        assertThat(tasks).hasSize(1);
        Task task = tasks.get(0);
        assertThat(task.getStatus()).isEqualTo(TaskStatus.QUEUED);
        assertThat(task.getIdempotencyKey()).isEqualTo(execId + ":Ocr:1");
        assertThat(task.getResource()).isEqualTo("ocr-handler");

        assertThat(redis.opsForStream().size("task:ocr-handler")).isGreaterThanOrEqualTo(1L);
    }

    @Test
    void taskSuccessDrivesMachineToSuccededTerminal() {
        String execId = seedRunningExecution();
        engineService.trigger(execId, new ExecutionStarted(new TextNode("scan.pdf")));

        engineService.trigger(execId, new TaskSucceeded("Ocr", new TextNode("EXTRACTED")));

        Execution exec = executionRepository.findById(execId).orElseThrow();
        assertThat(exec.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        assertThat(exec.getCurrentState()).isEqualTo("Done");
        assertThat(exec.getOutput()).isEqualTo(new TextNode("EXTRACTED"));
        assertThat(exec.getStoppedAt()).isNotNull();
        assertThat(eventRepository.findByExecutionIdOrderBySeqAsc(execId))
                .extracting(ExecutionEvent::getType)
                .containsExactly(EventType.EXECUTION_STARTED, EventType.STATE_ENTERED,
                        EventType.TASK_SCHEDULED, EventType.TASK_SUCCEEDED, EventType.STATE_EXITED,
                        EventType.STATE_ENTERED, EventType.EXECUTION_SUCCEEDED);
    }

    @Test
    void duplicateResultForTerminalExecutionIsNoOp() {
        String execId = seedRunningExecution();
        engineService.trigger(execId, new ExecutionStarted(new TextNode("scan.pdf")));
        engineService.trigger(execId, new TaskSucceeded("Ocr", new TextNode("EXTRACTED")));
        int before = eventRepository.findByExecutionIdOrderBySeqAsc(execId).size();

        engineService.trigger(execId, new TaskSucceeded("Ocr", new TextNode("EXTRACTED")));

        assertThat(eventRepository.findByExecutionIdOrderBySeqAsc(execId)).hasSize(before);
    }

    private String seedRunningExecution() {
        WorkflowDefinition def = definitionRepository.save(WorkflowDefinition.builder()
                .name("engine-it-" + System.nanoTime()).version(1)
                .definition(parse(MACHINE)).build());
        Execution exec = Execution.builder()
                .workflowDefinitionId(def.getId())
                .status(ExecutionStatus.RUNNING)
                .input(new TextNode("scan.pdf"))
                .build();
        exec.setName(exec.getId());
        exec.setRootExecutionId(exec.getId());
        return executionRepository.save(exec).getId();
    }

    private com.fasterxml.jackson.databind.JsonNode parse(String json) {
        try {
            return mapper.readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
