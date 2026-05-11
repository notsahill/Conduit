package com.example.conduit.repository;

import com.example.conduit.model.WorkflowDefinitions;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

@Repository
public interface WorkflowDefinitionsRepository extends JpaRepository<WorkflowDefinitions, String>, JpaSpecificationExecutor<WorkflowDefinitions> {
}
