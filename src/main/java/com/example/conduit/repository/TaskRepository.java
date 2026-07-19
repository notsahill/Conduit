package com.example.conduit.repository;

import com.example.conduit.model.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TaskRepository extends JpaRepository<Task, String>, JpaSpecificationExecutor<Task> {

    List<Task> findByExecutionId(String executionId);

    /** Per-attempt dedup key {@code executionId:stateName:attempt} — unique in the schema. */
    Optional<Task> findByIdempotencyKey(String idempotencyKey);
}
