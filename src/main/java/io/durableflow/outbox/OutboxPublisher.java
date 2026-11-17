package io.durableflow.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.domain.Models;
import io.durableflow.persistence.JdbcWorkflowStore;
import io.durableflow.queue.RedisStreams;
import io.durableflow.telemetry.Metrics;

public final class OutboxPublisher {
    private final JdbcWorkflowStore store;
    private final RedisStreams redis;
    private final ObjectMapper json;

    public OutboxPublisher(JdbcWorkflowStore store, RedisStreams redis, ObjectMapper json) {
        this.store = store;
        this.redis = redis;
        this.json = json;
    }

    public int publishOnce() {
        int due = store.enqueueDueRetries(20);
        if (due > 0) Metrics.RETRIES_ENQUEUED.labels("api").inc(due);
        int published = 0;
        for (var event : store.listPendingOutbox(20)) {
            final Models.TaskMessage payload;
            try {
                payload = json.treeToValue(event.payload(), Models.TaskMessage.class);
            } catch (Exception error) {
                store.recordOutboxFailure(event.id(), error.getMessage());
                continue;
            }
            try {
                redis.dispatch(payload);
            } catch (Exception error) {
                Metrics.DISPATCH.labels("api", "failed").inc();
                store.recordOutboxFailure(event.id(), error.getMessage());
                continue;
            }
            try { store.markOutboxDispatched(event.id()); }
            catch (RuntimeException error) {
                Metrics.DISPATCH.labels("api", "persist_error").inc();
                throw error;
            }
            Metrics.DISPATCH.labels("api", "published").inc();
            published++;
        }
        return published;
    }
}
