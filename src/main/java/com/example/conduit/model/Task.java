package com.example.conduit.model;

import com.example.conduit.common.model.AuditableEntity;
import com.example.conduit.enums.TaskStatus;
import com.example.conduit.enums.TaskType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.databind.JsonNode;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "tasks",
        uniqueConstraints = @UniqueConstraint(name = "uq_tasks_idempotency_key",
                columnNames = "idempotency_key"))
@Data
@EqualsAndHashCode(callSuper = true)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Task extends AuditableEntity {

    @Column(name = "execution_id", nullable = false)
    private String executionId;

    @Column(name = "state_name", nullable = false)
    private String stateName;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false)
    private TaskType type;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private TaskStatus status;

    @Column(name = "idempotency_key")
    private String idempotencyKey;

    @Column(name = "attempt")
    private Integer attempt;

    @Column(name = "max_attempts")
    private Integer maxAttempts;

    @Column(name = "next_run_at")
    private Instant nextRunAt;

    @Column(name = "redis_entry_id")
    private String redisEntryId;

    @Column(name = "resource")
    private String resource;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "parameters", columnDefinition = "jsonb")
    private JsonNode parameters;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "input", columnDefinition = "jsonb")
    private JsonNode input;

}
