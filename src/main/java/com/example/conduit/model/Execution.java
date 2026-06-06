package com.example.conduit.model;

import com.example.conduit.common.model.AuditableEntity;
import com.example.conduit.enums.ExecutionStatus;
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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "executions",
        uniqueConstraints = @UniqueConstraint(name = "uq_executions_workflow_definition_id_name",
                columnNames = {"workflow_definition_id", "name"}))
@Data
@EqualsAndHashCode(callSuper = true)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Execution extends AuditableEntity {

    @Column(name = "workflow_definition_id", nullable = false)
    private String workflowDefinitionId;

    @Column(name = "name", nullable = false)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private ExecutionStatus status;

    @Column(name = "current_state")
    private String currentState;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "input", nullable = false, columnDefinition = "jsonb")
    private Object input;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "output", columnDefinition = "jsonb")
    private Object output;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "error", columnDefinition = "jsonb")
    private Object error;

    @Column(name = "parent_execution_id")
    private String parentExecutionId;

    @Column(name = "parent_branch_index")
    private Integer parentBranchIndex;

    @Column(name = "root_execution_id", nullable = false)
    private String rootExecutionId;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "stopped_at")
    private Instant stoppedAt;

}
