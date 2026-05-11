package com.example.conduit.repository;

import com.example.conduit.model.WorkflowEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

@Repository
public interface WorkflowEventRepository extends JpaRepository<WorkflowEvent, Long>, JpaSpecificationExecutor<WorkflowEvent> {
}
