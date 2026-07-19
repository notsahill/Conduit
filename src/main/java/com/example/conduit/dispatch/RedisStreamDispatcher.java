package com.example.conduit.dispatch;

import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * v1 dispatch: {@code XADD task:<resource>} one entry per task attempt. One stream per resource;
 * workers join a per-resource consumer group and compete for entries.
 */
@Component
public class RedisStreamDispatcher implements TaskDispatcher {

    static final String STREAM_PREFIX = "task:";

    private final StringRedisTemplate redis;

    public RedisStreamDispatcher(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public void dispatch(TaskContext ctx) {
        Map<String, String> fields = new HashMap<>();
        fields.put("taskId", ctx.taskId());
        fields.put("executionId", ctx.executionId());
        fields.put("stateName", ctx.stateName());
        fields.put("resource", ctx.resource());
        fields.put("attempt", Integer.toString(ctx.attempt()));
        fields.put("idempotencyKey", ctx.idempotencyKey());
        fields.put("input", ctx.input() == null ? "null" : ctx.input().toString());
        fields.put("parameters", ctx.parameters() == null ? "null" : ctx.parameters().toString());
        redis.opsForStream().add(StreamRecords.mapBacked(fields).withStreamKey(streamKey(ctx.resource())));
    }

    static String streamKey(String resource) {
        return STREAM_PREFIX + resource;
    }
}
