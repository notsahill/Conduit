package com.example.conduit.api;

import com.example.conduit.api.dto.DlqEntryView;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/** Reads the dead-letter stream for inspection. */
@Service
public class DlqService {

    static final String DLQ_STREAM = "dlq";

    private final StringRedisTemplate redis;

    public DlqService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public List<DlqEntryView> list() {
        List<MapRecord<String, Object, Object>> records = redis.opsForStream().range(DLQ_STREAM, Range.unbounded());
        if (records == null) {
            return List.of();
        }
        return records.stream().map(this::toView).toList();
    }

    private DlqEntryView toView(MapRecord<String, Object, Object> record) {
        Map<Object, Object> f = record.getValue();
        return new DlqEntryView(record.getId().getValue(), str(f, "executionId"), str(f, "stateName"),
                str(f, "attempt"), str(f, "error"), str(f, "cause"));
    }

    private String str(Map<Object, Object> fields, String key) {
        Object v = fields.get(key);
        return v == null ? null : v.toString();
    }
}
