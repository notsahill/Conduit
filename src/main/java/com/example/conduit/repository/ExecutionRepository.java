package com.example.conduit.repository;

import com.example.conduit.model.Execution;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ExecutionRepository extends JpaRepository<Execution, String>, JpaSpecificationExecutor<Execution> {

    /**
     * Loads an execution under a {@code SELECT ... FOR UPDATE} row lock — the primary serialization
     * for concurrent triggers on one execution (gap #1). Holders queue; no two {@code decide()}
     * results interleave for the same execution.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select e from Execution e where e.id = :id")
    Optional<Execution> findByIdForUpdate(@Param("id") String id);

    /** Guards child spawning so a re-dispatched Parallel/Map fan-out never double-spawns an index. */
    boolean existsByParentExecutionIdAndBranchStateAndParentBranchIndex(
            String parentExecutionId, String branchState, Integer parentBranchIndex);
}
