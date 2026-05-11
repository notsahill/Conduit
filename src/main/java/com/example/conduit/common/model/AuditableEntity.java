package com.example.conduit.common.model;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.time.Instant;

@MappedSuperclass
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class AuditableEntity extends KeyedEntity {

    @Column(updatable = false)
    private Instant creationTime;

    private Instant lastUpdatedTime;

    @PreUpdate
    void updateAuditData() {
        this.lastUpdatedTime = Instant.now();
    }

    @PrePersist
    void setAuditData() {
        Instant now = Instant.now();
        // This allows inheritors to explicitly set the creationTime
        if (this.creationTime == null) {
            this.creationTime = now;
        }
        this.lastUpdatedTime = now;
    }
}