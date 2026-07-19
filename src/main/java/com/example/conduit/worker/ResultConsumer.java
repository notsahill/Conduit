package com.example.conduit.worker;

import com.example.conduit.engine.EngineService;
import com.example.conduit.engine.TaskFailed;
import com.example.conduit.engine.TaskSucceeded;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * The engine side of the results stream: reads worker outcomes and turns each into a
 * {@link TaskSucceeded}/{@link TaskFailed} trigger on the engine loop, then acks. The loop's
 * per-execution serialization + idempotency guard make a duplicate or stale result harmless.
 */
@Component
public class ResultConsumer {

    private static final Logger log = LoggerFactory.getLogger(ResultConsumer.class);
    static final String RESULTS_STREAM = "results";
    static final String GROUP = "engine";

    private final StringRedisTemplate redis;
    private final EngineService engineService;
    private final ObjectMapper mapper;

    public ResultConsumer(StringRedisTemplate redis, EngineService engineService, ObjectMapper mapper) {
        this.redis = redis;
        this.engineService = engineService;
        this.mapper = mapper;
    }

    /** Reads and applies up to a batch of pending results. Returns the count handled. */
    public int poll(String consumer) {
        ensureGroup();
        List<MapRecord<String, Object, Object>> records = redis.opsForStream().read(
                Consumer.from(GROUP, consumer),
                StreamReadOptions.empty().count(16),
                StreamOffset.create(RESULTS_STREAM, ReadOffset.lastConsumed()));
        if (records == null) {
            return 0;
        }
        for (MapRecord<String, Object, Object> record : records) {
            apply(record);
            redis.opsForStream().acknowledge(RESULTS_STREAM, GROUP, record.getId());
        }
        return records.size();
    }

    private void apply(MapRecord<String, Object, Object> record) {
        Map<Object, Object> f = record.getValue();
        String executionId = str(f, "executionId");
        String stateName = str(f, "stateName");
        String status = str(f, "status");
        if ("SUCCEEDED".equals(status)) {
            engineService.trigger(executionId, new TaskSucceeded(stateName, readJson(str(f, "output"))));
        } else {
            engineService.trigger(executionId, new TaskFailed(stateName, str(f, "error"), str(f, "cause")));
        }
    }

    private void ensureGroup() {
        try {
            redis.opsForStream().createGroup(RESULTS_STREAM, ReadOffset.from("0"), GROUP);
        } catch (Exception ignored) {
            // group already exists, or stream not created yet (nothing to read)
        }
    }

    private JsonNode readJson(String raw) {
        try {
            return raw == null ? mapper.nullNode() : mapper.readTree(raw);
        } catch (Exception e) {
            log.warn("bad result output json: {}", raw);
            return mapper.nullNode();
        }
    }

    private String str(Map<Object, Object> fields, String key) {
        Object v = fields.get(key);
        return v == null ? null : v.toString();
    }
}
