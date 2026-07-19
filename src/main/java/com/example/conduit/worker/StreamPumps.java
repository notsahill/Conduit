package com.example.conduit.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Drives the poll loops in the background for a running app ({@code bootRun} / the {@code app} compose
 * profile): each registered resource's task stream plus the results stream. Disabled in tests
 * ({@code conduit.streams.autostart=false}) so they can pump deterministically via explicit polls.
 */
@Component
public class StreamPumps implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(StreamPumps.class);

    private final WorkerRuntime worker;
    private final ResultConsumer resultConsumer;
    private final boolean autostart;
    private final String consumerId = "pump-" + Long.toString(System.nanoTime(), 36);

    private volatile boolean running;
    private Thread thread;

    public StreamPumps(WorkerRuntime worker, ResultConsumer resultConsumer,
                       @Value("${conduit.streams.autostart:true}") boolean autostart) {
        this.worker = worker;
        this.resultConsumer = resultConsumer;
        this.autostart = autostart;
    }

    @Override
    public void start() {
        running = true;
        thread = new Thread(this::loop, "conduit-stream-pumps");
        thread.setDaemon(true);
        thread.start();
        log.info("stream pumps started (consumer={})", consumerId);
    }

    private static final long IDLE_BACKOFF_MS = 200;
    private static final long ERROR_BACKOFF_MS = 2000;

    private void loop() {
        while (running) {
            try {
                int handled = 0;
                for (String resource : worker.registeredResources()) {
                    handled += worker.poll(resource, consumerId);
                }
                handled += resultConsumer.poll(consumerId);
                if (handled == 0) {
                    sleep(IDLE_BACKOFF_MS); // idle backoff
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // Back off on error (e.g. Redis unreachable) so a persistent failure doesn't tight-loop
                // and flood the logs; the pump retries once the dependency recovers.
                log.warn("stream pump cycle error, backing off {}ms: {}", ERROR_BACKOFF_MS, e.toString());
                try {
                    sleep(ERROR_BACKOFF_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private static void sleep(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    @Override
    public void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return autostart;
    }
}
