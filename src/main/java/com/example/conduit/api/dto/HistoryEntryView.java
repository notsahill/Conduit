package com.example.conduit.api.dto;

import com.example.conduit.enums.EventType;
import com.example.conduit.model.ExecutionEvent;

import java.time.Instant;

/** One entry of the ordered execution history (the audit trail). */
public record HistoryEntryView(
        int seq,
        EventType type,
        String stateName,
        Object payload,
        Instant createdAt
) {
    public static HistoryEntryView of(ExecutionEvent e) {
        return new HistoryEntryView(e.getSeq(), e.getType(), e.getStateName(), e.getPayload(), e.getCreatedAt());
    }
}
