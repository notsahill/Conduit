package com.example.conduit.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The worker side of the Redis Streams protocol: one consumer group per resource competes for task
 * entries, runs the registered {@link TaskHandler}, and reports the outcome on the shared results
 * stream. Dedup by idempotency key gives effectively-once delivery of the same attempt.
 */
@Component
public class WorkerRuntime {

    private static final Logger log = LoggerFactory.getLogger(WorkerRuntime.class);
    static final String RESULTS_STREAM = "results";

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();
    private final Set<String> processedKeys = ConcurrentHashMap.newKeySet();

    public WorkerRuntime(StringRedisTemplate redis, ObjectMapper mapper) {
        this.redis = redis;
        this.mapper = mapper;
    }

    public void register(String resource, TaskHandler handler) {
        handlers.put(resource, handler);
    }

    public Set<String> registeredResources() {
        return handlers.keySet();
    }

    /** Reads and processes up to a batch of pending entries for one resource. Returns the count handled. */
    public int poll(String resource, String consumer) {
        String stream = "task:" + resource;
        String group = resource + "-workers";
        ensureGroup(stream, group);

        List<MapRecord<String, Object, Object>> records = redis.opsForStream().read(
                Consumer.from(group, consumer),
                StreamReadOptions.empty().count(16),
                StreamOffset.create(stream, ReadOffset.lastConsumed()));
        if (records == null) {
            return 0;
        }
        for (MapRecord<String, Object, Object> record : records) {
            process(resource, stream, group, record);
        }
        return records.size();
    }

    /**
     * Reclaims entries left pending by a crashed worker: any that have been idle at least
     * {@code minIdleMs} since last delivery are XCLAIMed to {@code consumer} and reprocessed. This is
     * the crash-recovery path — a worker that died after read, before ack, no longer strands its task.
     */
    public int reclaim(String resource, String consumer, long minIdleMs) {
        String stream = "task:" + resource;
        String group = resource + "-workers";
        ensureGroup(stream, group);

        PendingMessages pending = redis.opsForStream().pending(stream, group, Range.unbounded(), 100);
        List<RecordId> stale = new ArrayList<>();
        for (PendingMessage message : pending) {
            if (message.getElapsedTimeSinceLastDelivery().toMillis() >= minIdleMs) {
                stale.add(message.getId());
            }
        }
        if (stale.isEmpty()) {
            return 0;
        }
        List<MapRecord<String, Object, Object>> claimed = redis.opsForStream().claim(
                stream, group, consumer, Duration.ofMillis(minIdleMs), stale.toArray(new RecordId[0]));
        for (MapRecord<String, Object, Object> record : claimed) {
            process(resource, stream, group, record);
        }
        return claimed.size();
    }

    private void process(String resource, String stream, String group, MapRecord<String, Object, Object> record) {
        Map<Object, Object> f = record.getValue();
        String idempotencyKey = str(f, "idempotencyKey");
        if (!processedKeys.add(idempotencyKey)) {
            redis.opsForStream().acknowledge(stream, group, record.getId());
            return; // duplicate delivery of the same attempt — effectively-once
        }

        ObjectNode result = mapper.createObjectNode();
        result.put("taskId", str(f, "taskId"));
        result.put("executionId", str(f, "executionId"));
        result.put("stateName", str(f, "stateName"));
        result.put("attempt", str(f, "attempt"));
        try {
            TaskHandler handler = handlers.get(resource);
            if (handler == null) {
                throw new IllegalStateException("no handler registered for resource '" + resource + "'");
            }
            JsonNode output = handler.handle(readJson(str(f, "input")));
            result.put("status", "SUCCEEDED");
            result.set("output", output == null ? mapper.nullNode() : output);
        } catch (Exception e) {
            log.warn("handler for resource {} failed: {}", resource, e.toString());
            result.put("status", "FAILED");
            result.put("error", e.getClass().getSimpleName());
            result.put("cause", e.getMessage());
        }
        report(result);
        redis.opsForStream().acknowledge(stream, group, record.getId());
    }

    private void report(ObjectNode result) {
        Map<String, String> fields = new HashMap<>();
        result.fields().forEachRemaining(e ->
                fields.put(e.getKey(), e.getValue().isTextual() ? e.getValue().asText() : e.getValue().toString()));
        redis.opsForStream().add(StreamRecords.mapBacked(fields).withStreamKey(RESULTS_STREAM));
    }

    private void ensureGroup(String stream, String group) {
        try {
            redis.opsForStream().createGroup(stream, ReadOffset.from("0"), group);
        } catch (Exception ignored) {
            // group already exists, or stream not created yet (nothing to read)
        }
    }

    private JsonNode readJson(String raw) {
        try {
            return raw == null ? mapper.nullNode() : mapper.readTree(raw);
        } catch (Exception e) {
            throw new IllegalArgumentException("bad task input json: " + raw, e);
        }
    }

    private String str(Map<Object, Object> fields, String key) {
        Object v = fields.get(key);
        return v == null ? null : v.toString();
    }
}
