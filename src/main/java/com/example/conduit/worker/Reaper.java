package com.example.conduit.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Crash recovery: periodically reclaims task-stream entries that a worker read but never acked (it
 * died mid-task). Reassigns anything idle beyond the threshold back into processing. Disabled in tests
 * ({@code conduit.streams.autostart=false}), which reclaim explicitly.
 */
@Component
public class Reaper {

    private static final Logger log = LoggerFactory.getLogger(Reaper.class);

    private final WorkerRuntime worker;
    private final boolean autostart;
    private final long minIdleMs;
    private final String consumerId = "reaper-" + Long.toString(System.nanoTime(), 36);

    public Reaper(WorkerRuntime worker,
                  @Value("${conduit.streams.autostart:true}") boolean autostart,
                  @Value("${conduit.reaper.min-idle-ms:30000}") long minIdleMs) {
        this.worker = worker;
        this.autostart = autostart;
        this.minIdleMs = minIdleMs;
    }

    @Scheduled(fixedDelayString = "${conduit.reaper.interval-ms:15000}")
    void scheduledReap() {
        if (!autostart) {
            return;
        }
        for (String resource : worker.registeredResources()) {
            try {
                int reclaimed = worker.reclaim(resource, consumerId, minIdleMs);
                if (reclaimed > 0) {
                    log.info("reaper reclaimed {} stuck task(s) for resource {}", reclaimed, resource);
                }
            } catch (Exception e) {
                log.warn("reaper error for resource {}: {}", resource, e.toString());
            }
        }
    }
}
