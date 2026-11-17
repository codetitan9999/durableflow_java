package io.durableflow.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.domain.Models;
import java.sql.SQLException;
import java.util.UUID;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcIdempotencyStore implements IdempotencyStore {
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;

    public JdbcIdempotencyStore(JdbcTemplate jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @Override
    public Models.IdempotencyResult beginIdempotentTask(String handlerKey, String key, String ownerTaskId) {
        int inserted = jdbc.update("INSERT INTO idempotency_records (handler_key, idempotency_key, owner_task_instance_id, status, created_at, updated_at) VALUES (?, ?, ?, 'in_progress', NOW(), NOW()) ON CONFLICT (handler_key, idempotency_key) DO NOTHING",
                handlerKey, key, UUID.fromString(ownerTaskId));
        if (inserted == 1) return new Models.IdempotencyResult(null, false);
        return jdbc.queryForObject("SELECT status, response_json, owner_task_instance_id FROM idempotency_records WHERE handler_key = ? AND idempotency_key = ?",
                (rs, row) -> {
                    if ("completed".equals(rs.getString("status"))) {
                        String response = rs.getString("response_json");
                        try { return new Models.IdempotencyResult(response == null ? null : json.readTree(response), true); }
                        catch (JsonProcessingException error) { throw new IllegalStateException("Invalid cached response", error); }
                    }
                    Object owner = rs.getObject("owner_task_instance_id");
                    if (owner != null && ownerTaskId.equals(owner.toString())) return new Models.IdempotencyResult(null, false);
                    throw new InProgress();
                }, handlerKey, key);
    }

    @Override
    public void completeIdempotentTask(String handlerKey, String key, String ownerTaskId, JsonNode response) {
        int updated = jdbc.update("UPDATE idempotency_records SET status = 'completed', response_json = ?, updated_at = NOW() WHERE handler_key = ? AND idempotency_key = ? AND owner_task_instance_id = ?",
                jsonb(response), handlerKey, key, UUID.fromString(ownerTaskId));
        if (updated == 0) throw new StoreExceptions.NotFound();
    }

    @Override
    public void releaseIdempotentTask(String handlerKey, String key, String ownerTaskId) {
        jdbc.update("DELETE FROM idempotency_records WHERE handler_key = ? AND idempotency_key = ? AND status = 'in_progress' AND owner_task_instance_id = ?",
                handlerKey, key, UUID.fromString(ownerTaskId));
    }

    private PGobject jsonb(JsonNode value) {
        PGobject object = new PGobject();
        object.setType("jsonb");
        try { object.setValue(value == null ? "{}" : json.writeValueAsString(value)); }
        catch (JsonProcessingException | SQLException error) { throw new IllegalArgumentException("Invalid response JSON", error); }
        return object;
    }

    public static final class InProgress extends RuntimeException {
        public InProgress() { super("idempotency key is already in progress"); }
    }
}
