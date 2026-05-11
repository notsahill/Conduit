package com.example.conduit.model;

import com.example.conduit.common.model.AuditableEntity;
import com.example.conduit.enums.WorkflowStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "workflow_definitions")
@Data
@EqualsAndHashCode(callSuper = true)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowDefinitions extends AuditableEntity {

    @Column(name = "name",nullable = false)
    private String name;

    @Column(name = "type",nullable = false)
    private String type;

    @Column(name = "steps", nullable = false)
    private String[] steps;

}
