package com.example.conduit.repository;

import com.example.conduit.model.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface TaskRepository extends JpaRepository<Task, String>, JpaSpecificationExecutor<Task> {

    List<Task> findByExecutionId(String executionId);

    /** Per-attempt dedup key {@code executionId:stateName:attempt} — unique in the schema. */
    Optional<Task> findByIdempotencyKey(String idempotencyKey);

    /**
     * Claims due TIMER rows for exactly-one processing under overlapping poll cycles.
     * {@code FOR UPDATE SKIP LOCKED} lets concurrent pollers each grab a disjoint batch; the caller
     * flips {@code status} to QUEUED in the same transaction as the idempotency guard.
     */
    @Query(value = """
            SELECT * FROM tasks
            WHERE type = 'TIMER' AND status = 'SCHEDULED' AND next_run_at <= :now
            ORDER BY next_run_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<Task> claimDueTimers(@Param("now") Instant now, @Param("limit") int limit);
}
