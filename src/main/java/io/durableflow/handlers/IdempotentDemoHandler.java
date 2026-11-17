package io.durableflow.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.durableflow.domain.Models;
import io.durableflow.persistence.IdempotencyStore;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public final class IdempotentDemoHandler implements TaskHandler {
    private final String key;
    private final IdempotencyStore idempotency;
    private final ObjectMapper json;

    public IdempotentDemoHandler(String key, IdempotencyStore idempotency, ObjectMapper json) {
        if (!key.equals("sample.echo") && !key.equals("notifications.send")) throw new IllegalArgumentException("Unknown demo handler");
        this.key = key;
        this.idempotency = idempotency;
        this.json = json;
    }

    @Override
    public String key() { return key; }

    @Override
    public JsonNode handle(Models.TaskInstance task) {
        var reservation = idempotency.beginIdempotentTask(key, task.idempotencyKey(), task.id());
        if (reservation.replayed()) return reservation.response();
        try {
            if (task.input() != null && !task.input().isNull() && !task.input().isObject()) {
                throw new IllegalArgumentException("task input must be a JSON object");
            }
            ObjectNode response = json.createObjectNode();
            response.put("handler", key);
            response.put("task_id", task.id());
            String timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS).toString();
            if (key.equals("sample.echo")) {
                response.put("processed_at", timestamp);
                response.set("echo", task.input());
            } else {
                response.put("notification_id", task.idempotencyKey());
                response.put("sent_at", timestamp);
                response.set("payload", task.input());
            }
            idempotency.completeIdempotentTask(key, task.idempotencyKey(), task.id(), response);
            return response;
        } catch (RuntimeException error) {
            idempotency.releaseIdempotentTask(key, task.idempotencyKey(), task.id());
            throw error;
        }
    }
}
