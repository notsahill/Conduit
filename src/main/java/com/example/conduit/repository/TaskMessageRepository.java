package com.example.conduit.repository;

import com.example.conduit.model.TaskMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

@Repository
public interface TaskMessageRepository extends JpaRepository<TaskMessage, String>, JpaSpecificationExecutor<TaskMessage> {
}
