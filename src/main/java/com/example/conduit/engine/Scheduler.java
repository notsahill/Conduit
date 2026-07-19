package com.example.conduit.engine;

import com.example.conduit.enums.TaskStatus;
import com.example.conduit.model.Task;
import com.example.conduit.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;

/**
 * Claims due TIMER rows and resumes their executions. The claim uses {@code SELECT ... FOR UPDATE SKIP
 * LOCKED} + a status flip in one transaction, so overlapping poll cycles (and the reaper) never
 * double-fire the same timer. Triggers run after the claim commits — each takes the engine's own
 * per-execution lock. Disabled in tests ({@code conduit.streams.autostart=false}) so they poll explicitly.
 */
@Component
public class Scheduler {

    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);
    private static final int BATCH = 100;

    private final TaskRepository taskRepository;
    private final EngineService engineService;
    private final TransactionTemplate txTemplate;
    private final boolean autostart;

    public Scheduler(TaskRepository taskRepository, EngineService engineService,
                     PlatformTransactionManager txManager,
                     @Value("${conduit.streams.autostart:true}") boolean autostart) {
        this.taskRepository = taskRepository;
        this.engineService = engineService;
        this.txTemplate = new TransactionTemplate(txManager);
        this.autostart = autostart;
    }

    @Scheduled(fixedDelayString = "${conduit.scheduler.interval-ms:1000}")
    void scheduledPoll() {
        if (autostart) {
            pollOnce();
        }
    }

    /** Claims and fires all currently-due timers. Returns the count fired. */
    public int pollOnce() {
        List<ClaimedTimer> claimed = txTemplate.execute(status -> claimDue());
        if (claimed == null) {
            return 0;
        }
        for (ClaimedTimer timer : claimed) {
            try {
                engineService.trigger(timer.executionId(), timer.toTrigger());
            } catch (Exception e) {
                log.warn("timer trigger failed for execution {} state {}: {}",
                        timer.executionId(), timer.state(), e.toString());
            }
        }
        return claimed.size();
    }

    private List<ClaimedTimer> claimDue() {
        List<Task> due = taskRepository.claimDueTimers(Instant.now(), BATCH);
        for (Task timer : due) {
            timer.setStatus(TaskStatus.QUEUED); // status flip is the idempotency guard against re-claim
        }
        taskRepository.saveAll(due);
        return due.stream()
                .map(t -> new ClaimedTimer(t.getExecutionId(), t.getStateName(),
                        TimerKind.valueOf(t.getTimerKind()), t.getAttempt()))
                .toList();
    }

    private record ClaimedTimer(String executionId, String state, TimerKind kind, int attempt) {
        EngineEvent toTrigger() {
            return switch (kind) {
                case WAIT -> new WaitCompleted(state);
                case RETRY -> new RetryDue(state);
                case TIMEOUT -> new TaskTimedOut(state, attempt);
            };
        }
    }
}
