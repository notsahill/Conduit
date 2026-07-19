package com.example.conduit.repository;

import com.example.conduit.model.ExecutionEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ExecutionEventRepository extends JpaRepository<ExecutionEvent, Long>, JpaSpecificationExecutor<ExecutionEvent> {

    /** The ordered audit trail for an execution (dense seq, ascending). */
    List<ExecutionEvent> findByExecutionIdOrderBySeqAsc(String executionId);
}
