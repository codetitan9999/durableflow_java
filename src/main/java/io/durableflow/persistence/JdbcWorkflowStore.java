package io.durableflow.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.domain.Models;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.postgresql.util.PGobject;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.support.TransactionTemplate;

public final class JdbcWorkflowStore {
    private static final String DEFINITION_COLUMNS = "id, name, description, version, status, definition_json, created_at, updated_at";
    private static final String EXECUTION_COLUMNS = "id, workflow_definition_id, status, input_json, output_json, error_text, created_at, updated_at, started_at, completed_at";
    private static final String TASK_COLUMNS = "id, workflow_execution_id, task_name, handler_key, status, input_json, output_json, next_run_at, last_error_text, attempts_total, idempotency_key, dispatched_at, completed_at, created_at, updated_at";
    private static final String ATTEMPT_COLUMNS = "id, task_instance_id, attempt_number, status, started_at, finished_at, error_text, output_json, created_at";
    private static final String OUTBOX_COLUMNS = "id, aggregate_type, aggregate_id, event_type, payload_json, available_at, dispatched_at, attempt_count, last_error_text, created_at";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final ObjectMapper json;

    public JdbcWorkflowStore(JdbcTemplate jdbc, TransactionTemplate transactions, ObjectMapper json) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.json = json;
    }

    public Models.WorkflowDefinition createDefinition(String name, String description, JsonNode definition) {
        return jdbc.queryForObject("INSERT INTO workflow_definitions (name, description, definition_json, status) VALUES (?, ?, ?, 'active') RETURNING " + DEFINITION_COLUMNS,
                definitionMapper(), name, description, jsonb(definition));
    }

    public Models.WorkflowDefinition getDefinition(String id) {
        return required(() -> jdbc.queryForObject("SELECT " + DEFINITION_COLUMNS + " FROM workflow_definitions WHERE id = ?", definitionMapper(), UUID.fromString(id)));
    }

    public Models.WorkflowExecution getExecution(String id) {
        return required(() -> jdbc.queryForObject("SELECT " + EXECUTION_COLUMNS + " FROM workflow_executions WHERE id = ?", executionMapper(), UUID.fromString(id)));
    }

    public Models.TaskInstance getTask(String id) {
        return required(() -> jdbc.queryForObject("SELECT " + TASK_COLUMNS + " FROM task_instances WHERE id = ?", taskMapper(), UUID.fromString(id)));
    }

    public Models.ExecutionStartResult createExecutionAndTask(String definitionId, JsonNode input, String taskName, String handlerKey) {
        return transactions.execute(status -> {
            Models.WorkflowExecution execution = jdbc.queryForObject("INSERT INTO workflow_executions (workflow_definition_id, status, input_json, created_at, updated_at, started_at) VALUES (?, 'running', ?, NOW(), NOW(), NOW()) RETURNING " + EXECUTION_COLUMNS,
                    executionMapper(), UUID.fromString(definitionId), jsonb(input));
            Models.TaskInstance task = jdbc.queryForObject("INSERT INTO task_instances (workflow_execution_id, task_name, handler_key, status, input_json, idempotency_key, created_at, updated_at) VALUES (?, ?, ?, 'pending', ?, ?, NOW(), NOW()) RETURNING " + TASK_COLUMNS,
                    taskMapper(), UUID.fromString(execution.id()), taskName, handlerKey, jsonb(input), execution.id() + ":" + taskName);
            var payload = json.createObjectNode().put("task_id", task.id()).put("execution_id", execution.id()).put("handler_key", task.handlerKey());
            jdbc.update("INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload_json, available_at) VALUES ('task_instance', ?, 'task.dispatch', ?, NOW())",
                    UUID.fromString(task.id()), jsonb(payload));
            return new Models.ExecutionStartResult(execution, task);
        });
    }

    public Models.ExecutionSnapshot getSnapshot(String executionId) {
        Models.WorkflowExecution execution = getExecution(executionId);
        List<Models.TaskInstance> tasks = jdbc.query("SELECT " + TASK_COLUMNS + " FROM task_instances WHERE workflow_execution_id = ? ORDER BY created_at",
                taskMapper(), UUID.fromString(executionId));
        List<Models.TaskAttempt> attempts = jdbc.query("SELECT a." + ATTEMPT_COLUMNS.replace(", ", ", a.") + " FROM task_attempts a JOIN task_instances t ON t.id = a.task_instance_id WHERE t.workflow_execution_id = ? ORDER BY a.task_instance_id, a.attempt_number",
                attemptMapper(), UUID.fromString(executionId));
        Map<String, List<Models.TaskAttempt>> byTask = new HashMap<>();
        for (var attempt : attempts) {
            byTask.computeIfAbsent(attempt.taskInstanceId(), ignored -> new ArrayList<>()).add(attempt);
        }
        List<Models.TaskSnapshot> snapshots = new ArrayList<>();
        for (var task : tasks) {
            snapshots.add(new Models.TaskSnapshot(task, byTask.getOrDefault(task.id(), List.of())));
        }
        return new Models.ExecutionSnapshot(execution, snapshots);
    }

    public List<Models.TaskInstance> listDeadLettered(int limit) {
        return jdbc.query("SELECT " + TASK_COLUMNS + " FROM task_instances WHERE status = 'dead_lettered' ORDER BY updated_at DESC LIMIT ?",
                taskMapper(), limit);
    }

    public Models.TaskInstance replayDeadLettered(String taskId) {
        return transactions.execute(status -> {
            Models.TaskInstance task = required(() -> jdbc.queryForObject("SELECT " + TASK_COLUMNS + " FROM task_instances WHERE id = ? FOR UPDATE",
                    taskMapper(), UUID.fromString(taskId)));
            if (!"dead_lettered".equals(task.status())) throw new StoreExceptions.NotReplayable();
            Models.TaskInstance updated = jdbc.queryForObject("UPDATE task_instances SET status = 'pending', output_json = NULL, next_run_at = NULL, last_error_text = NULL, completed_at = NULL, updated_at = NOW() WHERE id = ? RETURNING " + TASK_COLUMNS,
                    taskMapper(), UUID.fromString(taskId));
            jdbc.update("UPDATE workflow_executions SET status = 'running', error_text = NULL, completed_at = NULL, updated_at = NOW() WHERE id = ?",
                    UUID.fromString(task.workflowExecutionId()));
            var payload = json.createObjectNode().put("task_id", updated.id()).put("execution_id", updated.workflowExecutionId()).put("handler_key", updated.handlerKey());
            jdbc.update("INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload_json, available_at) VALUES ('task_instance', ?, 'task.dispatch', ?, NOW())",
                    UUID.fromString(updated.id()), jsonb(payload));
            return updated;
        });
    }

    public Models.StartedAttempt startTaskAttempt(String taskId) {
        return transactions.execute(status -> {
            Models.TaskInstance task = required(() -> jdbc.queryForObject("SELECT " + TASK_COLUMNS + " FROM task_instances WHERE id = ? FOR UPDATE",
                    taskMapper(), UUID.fromString(taskId)));
            if ("succeeded".equals(task.status()) || "dead_lettered".equals(task.status())) {
                return new Models.StartedAttempt(task, null, true);
            }
            int number = task.attemptsTotal() + 1;
            Models.TaskAttempt attempt = jdbc.queryForObject("INSERT INTO task_attempts (task_instance_id, attempt_number, status, started_at, created_at) VALUES (?, ?, 'running', NOW(), NOW()) RETURNING " + ATTEMPT_COLUMNS,
                    attemptMapper(), UUID.fromString(taskId), number);
            Models.TaskInstance running = jdbc.queryForObject("UPDATE task_instances SET status = 'running', attempts_total = ?, next_run_at = NULL, updated_at = NOW() WHERE id = ? RETURNING " + TASK_COLUMNS,
                    taskMapper(), number, UUID.fromString(taskId));
            return new Models.StartedAttempt(running, attempt, false);
        });
    }

    public void completeTaskAttempt(String taskId, String attemptId, JsonNode output) {
        transactions.executeWithoutResult(status -> {
            String executionId = required(() -> jdbc.queryForObject("SELECT workflow_execution_id FROM task_instances WHERE id = ? FOR UPDATE", String.class, UUID.fromString(taskId)));
            var payload = jsonb(output);
            jdbc.update("UPDATE task_attempts SET status = 'succeeded', finished_at = NOW(), output_json = ? WHERE id = ?", payload, UUID.fromString(attemptId));
            jdbc.update("UPDATE task_instances SET status = 'succeeded', output_json = ?, completed_at = NOW(), updated_at = NOW() WHERE id = ?", payload, UUID.fromString(taskId));
            jdbc.update("UPDATE workflow_executions SET status = 'succeeded', output_json = ?, completed_at = NOW(), updated_at = NOW() WHERE id = ?", payload, UUID.fromString(executionId));
        });
    }

    public void completeTaskAttemptAndEnqueueNext(String taskId, String attemptId, String nextName, String nextHandler, JsonNode output) {
        transactions.executeWithoutResult(status -> {
            String executionId = required(() -> jdbc.queryForObject("SELECT workflow_execution_id FROM task_instances WHERE id = ? FOR UPDATE", String.class, UUID.fromString(taskId)));
            var payload = jsonb(output);
            jdbc.update("UPDATE task_attempts SET status = 'succeeded', finished_at = NOW(), output_json = ? WHERE id = ?", payload, UUID.fromString(attemptId));
            jdbc.update("UPDATE task_instances SET status = 'succeeded', output_json = ?, completed_at = NOW(), updated_at = NOW() WHERE id = ?", payload, UUID.fromString(taskId));
            String nextId = jdbc.queryForObject("INSERT INTO task_instances (workflow_execution_id, task_name, handler_key, status, input_json, idempotency_key, created_at, updated_at) VALUES (?, ?, ?, 'pending', ?, ?, NOW(), NOW()) RETURNING id",
                    String.class, UUID.fromString(executionId), nextName, nextHandler, payload, executionId + ":" + nextName);
            var dispatch = json.createObjectNode().put("task_id", nextId).put("execution_id", executionId).put("handler_key", nextHandler);
            jdbc.update("INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload_json, available_at) VALUES ('task_instance', ?, 'task.dispatch', ?, NOW())",
                    UUID.fromString(nextId), jsonb(dispatch));
        });
    }

    public void scheduleTaskRetry(String taskId, String attemptId, String error, Instant nextRunAt) {
        transactions.executeWithoutResult(status -> {
            String message = truncate(error);
            jdbc.update("UPDATE task_attempts SET status = 'failed', finished_at = NOW(), error_text = ? WHERE id = ?", message, UUID.fromString(attemptId));
            jdbc.update("UPDATE task_instances SET status = 'pending', last_error_text = ?, next_run_at = ?, updated_at = NOW() WHERE id = ?",
                    message, java.sql.Timestamp.from(nextRunAt), UUID.fromString(taskId));
        });
    }

    public void failTaskAttempt(String taskId, String attemptId, String error) {
        transactions.executeWithoutResult(status -> {
            String executionId = required(() -> jdbc.queryForObject("SELECT workflow_execution_id FROM task_instances WHERE id = ? FOR UPDATE", String.class, UUID.fromString(taskId)));
            String message = truncate(error);
            jdbc.update("UPDATE task_attempts SET status = 'failed', finished_at = NOW(), error_text = ? WHERE id = ?", message, UUID.fromString(attemptId));
            jdbc.update("UPDATE task_instances SET status = 'dead_lettered', last_error_text = ?, next_run_at = NULL, completed_at = NOW(), updated_at = NOW() WHERE id = ?", message, UUID.fromString(taskId));
            jdbc.update("UPDATE workflow_executions SET status = 'failed', error_text = ?, completed_at = NOW(), updated_at = NOW() WHERE id = ?", message, UUID.fromString(executionId));
        });
    }

    public int enqueueDueRetries(int limit) {
        return transactions.execute(status -> {
            var due = jdbc.query("SELECT id, workflow_execution_id, handler_key FROM task_instances WHERE status = 'pending' AND next_run_at IS NOT NULL AND next_run_at <= NOW() ORDER BY next_run_at LIMIT ? FOR UPDATE SKIP LOCKED",
                    (rs, row) -> new Models.TaskMessage(id(rs, "id"), id(rs, "workflow_execution_id"), rs.getString("handler_key")), limit);
            for (var task : due) {
                var payload = json.createObjectNode().put("task_id", task.taskId()).put("execution_id", task.executionId()).put("handler_key", task.handlerKey());
                jdbc.update("INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload_json, available_at) VALUES ('task_instance', ?, 'task.dispatch', ?, NOW())",
                        UUID.fromString(task.taskId()), jsonb(payload));
                jdbc.update("UPDATE task_instances SET next_run_at = NULL, updated_at = NOW() WHERE id = ?", UUID.fromString(task.taskId()));
            }
            return due.size();
        });
    }

    public List<Models.OutboxEvent> listPendingOutbox(int limit) {
        return jdbc.query("SELECT " + OUTBOX_COLUMNS + " FROM outbox_events WHERE dispatched_at IS NULL AND available_at <= NOW() ORDER BY created_at LIMIT ?",
                (rs, row) -> new Models.OutboxEvent(id(rs, "id"), rs.getString("aggregate_type"), id(rs, "aggregate_id"),
                        rs.getString("event_type"), readJson(rs, "payload_json"), instant(rs, "available_at"), instant(rs, "dispatched_at"),
                        rs.getInt("attempt_count"), rs.getString("last_error_text"), instant(rs, "created_at")), limit);
    }

    public void markOutboxDispatched(String id) {
        jdbc.update("UPDATE outbox_events SET dispatched_at = NOW(), attempt_count = attempt_count + 1 WHERE id = ?", UUID.fromString(id));
    }

    public void recordOutboxFailure(String id, String error) {
        jdbc.update("UPDATE outbox_events SET attempt_count = attempt_count + 1, last_error_text = ? WHERE id = ?", truncate(error), UUID.fromString(id));
    }

    private static String truncate(String text) {
        return text == null ? "" : text.substring(0, Math.min(1000, text.length()));
    }

    private RowMapper<Models.WorkflowDefinition> definitionMapper() {
        return (rs, row) -> new Models.WorkflowDefinition(id(rs, "id"), rs.getString("name"), rs.getString("description"),
                rs.getInt("version"), rs.getString("status"), readJson(rs, "definition_json"), instant(rs, "created_at"), instant(rs, "updated_at"));
    }

    private RowMapper<Models.WorkflowExecution> executionMapper() {
        return (rs, row) -> new Models.WorkflowExecution(id(rs, "id"), id(rs, "workflow_definition_id"), rs.getString("status"),
                readJson(rs, "input_json"), readJson(rs, "output_json"), rs.getString("error_text"), instant(rs, "created_at"),
                instant(rs, "updated_at"), instant(rs, "started_at"), instant(rs, "completed_at"));
    }

    private RowMapper<Models.TaskInstance> taskMapper() {
        return (rs, row) -> new Models.TaskInstance(id(rs, "id"), id(rs, "workflow_execution_id"), rs.getString("task_name"),
                rs.getString("handler_key"), rs.getString("status"), readJson(rs, "input_json"), readJson(rs, "output_json"),
                instant(rs, "next_run_at"), rs.getString("last_error_text"), rs.getInt("attempts_total"), rs.getString("idempotency_key"),
                instant(rs, "dispatched_at"), instant(rs, "completed_at"), instant(rs, "created_at"), instant(rs, "updated_at"));
    }

    private RowMapper<Models.TaskAttempt> attemptMapper() {
        return (rs, row) -> new Models.TaskAttempt(id(rs, "id"), id(rs, "task_instance_id"), rs.getInt("attempt_number"), rs.getString("status"),
                instant(rs, "started_at"), instant(rs, "finished_at"), rs.getString("error_text"), readJson(rs, "output_json"), instant(rs, "created_at"));
    }

    private String id(ResultSet rs, String column) throws SQLException {
        Object value = rs.getObject(column);
        return value == null ? null : value.toString();
    }

    private Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private JsonNode readJson(ResultSet rs, String column) throws SQLException {
        String value = rs.getString(column);
        if (value == null) return null;
        try {
            return json.readTree(value);
        } catch (JsonProcessingException error) {
            throw new SQLException("Invalid JSON in " + column, error);
        }
    }

    private PGobject jsonb(JsonNode value) {
        PGobject object = new PGobject();
        object.setType("jsonb");
        try {
            object.setValue(value == null ? "{}" : json.writeValueAsString(value));
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("Invalid JSON value", error);
        } catch (SQLException error) {
            throw new IllegalStateException(error);
        }
        return object;
    }

    private <T> T required(java.util.function.Supplier<T> query) {
        try {
            return query.get();
        } catch (EmptyResultDataAccessException error) {
            throw new StoreExceptions.NotFound();
        }
    }
}
