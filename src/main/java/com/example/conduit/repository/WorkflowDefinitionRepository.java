package com.example.conduit.repository;

import com.example.conduit.model.WorkflowDefinition;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface WorkflowDefinitionRepository extends JpaRepository<WorkflowDefinition, String>, JpaSpecificationExecutor<WorkflowDefinition> {

    /** Highest existing version for a name — the basis for the next version on re-create. */
    Optional<WorkflowDefinition> findFirstByNameOrderByVersionDesc(String name);
}
