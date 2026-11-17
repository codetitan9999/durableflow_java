package io.durableflow.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.domain.Models;
import io.durableflow.handlers.TaskHandler;
import io.durableflow.handlers.IdempotentDemoHandler;
import io.durableflow.orchestrator.TaskWorker;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class JdbcWorkflowStoreIntegrationTest {
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");
    static JdbcWorkflowStore store;
    static JdbcTemplate jdbc;
    static ObjectMapper json = new ObjectMapper();

    @BeforeAll
    static void setup() throws Exception {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute(Files.readString(Path.of("migrations/001_init.sql")));
        jdbc.execute(Files.readString(Path.of("migrations/002_idempotency_records.sql")));
        jdbc.execute(Files.readString(Path.of("migrations/003_idempotency_record_ownership.sql")));
        store = new JdbcWorkflowStore(jdbc, new TransactionTemplate(new DataSourceTransactionManager(dataSource)), json);
    }

    @Test
    void createsExecutionTaskAndOutboxAtomicallyAndReadsSnapshot() throws Exception {
        var definition = store.createDefinition("integration-" + UUID.randomUUID(), "test", json.readTree("""
                {"entry_task":"first","tasks":[{"name":"first","handler_key":"sample.echo"}]}
                """));
        var result = store.createExecutionAndTask(definition.id(), json.readTree("{\"order_id\":\"42\"}"), "first", "sample.echo");
        assertEquals("running", result.execution().status());
        assertEquals("pending", result.task().status());
        assertEquals(result.execution().id() + ":first", result.task().idempotencyKey());
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE aggregate_id = ?::uuid", Integer.class, result.task().id()));
        var snapshot = store.getSnapshot(result.execution().id());
        assertEquals(1, snapshot.tasks().size());
        assertTrue(snapshot.tasks().getFirst().attempts().isEmpty());
    }

    @Test
    void replayResetsTaskAndExecutionAndEnqueuesDispatch() throws Exception {
        var definition = store.createDefinition("replay-" + UUID.randomUUID(), "test", json.readTree("""
                {"entry_task":"first","tasks":[{"name":"first","handler_key":"sample.echo"}]}
                """));
        var result = store.createExecutionAndTask(definition.id(), json.readTree("{}"), "first", "sample.echo");
        jdbc.update("UPDATE task_instances SET status = 'dead_lettered', attempts_total = 2, last_error_text = 'failed' WHERE id = ?::uuid", result.task().id());
        jdbc.update("UPDATE workflow_executions SET status = 'failed', error_text = 'failed' WHERE id = ?::uuid", result.execution().id());
        Models.TaskInstance replayed = store.replayDeadLettered(result.task().id());
        assertEquals("pending", replayed.status());
        assertEquals(2, replayed.attemptsTotal());
        assertNull(replayed.lastError());
        assertEquals("running", store.getExecution(result.execution().id()).status());
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE aggregate_id = ?::uuid", Integer.class, result.task().id()));
        assertThrows(StoreExceptions.NotReplayable.class, () -> store.replayDeadLettered(result.task().id()));
    }

    @Test
    void attemptRecoveryAndChainingPersistInOneTransaction() throws Exception {
        var definition = store.createDefinition("chain-" + UUID.randomUUID(), "test", json.readTree("""
                {"entry_task":"first","tasks":[{"name":"first","handler_key":"sample.echo","next_task":"second"},{"name":"second","handler_key":"sample.echo"}]}
                """));
        var result = store.createExecutionAndTask(definition.id(), json.readTree("{}"), "first", "sample.echo");
        var first = store.startTaskAttempt(result.task().id());
        var recovered = store.startTaskAttempt(result.task().id());
        assertEquals(1, first.attempt().attemptNumber());
        assertEquals(2, recovered.attempt().attemptNumber());
        store.completeTaskAttemptAndEnqueueNext(result.task().id(), recovered.attempt().id(), "second", "sample.echo", json.readTree("{\"done\":true}"));
        var snapshot = store.getSnapshot(result.execution().id());
        assertEquals(2, snapshot.tasks().size());
        assertEquals("succeeded", snapshot.tasks().getFirst().task().status());
        assertEquals("pending", snapshot.tasks().get(1).task().status());
        assertEquals(json.readTree("{\"done\":true}"), snapshot.tasks().get(1).task().input());
        assertTrue(store.startTaskAttempt(result.task().id()).alreadyCompleted());
    }

    @Test
    void dueRetryCreatesOutboxOnlyOnceAndTracksPublishResult() throws Exception {
        var definition = store.createDefinition("retry-" + UUID.randomUUID(), "test", json.readTree("""
                {"entry_task":"first","tasks":[{"name":"first","handler_key":"sample.echo"}]}
                """));
        var result = store.createExecutionAndTask(definition.id(), json.readTree("{}"), "first", "sample.echo");
        var attempt = store.startTaskAttempt(result.task().id());
        store.scheduleTaskRetry(result.task().id(), attempt.attempt().id(), "temporary", java.time.Instant.now().minusSeconds(1));
        assertEquals(1, store.enqueueDueRetries(20));
        assertEquals(0, store.enqueueDueRetries(20));
        var events = store.listPendingOutbox(20).stream().filter(e -> e.aggregateId().equals(result.task().id())).toList();
        assertEquals(2, events.size());
        store.recordOutboxFailure(events.getFirst().id(), "redis unavailable");
        assertEquals(1, store.listPendingOutbox(20).stream().filter(e -> e.id().equals(events.getFirst().id())).findFirst().orElseThrow().attemptCount());
        store.markOutboxDispatched(events.getFirst().id());
        assertEquals(1, store.listPendingOutbox(20).stream().filter(e -> e.aggregateId().equals(result.task().id())).count());
    }

    @Test
    void idempotencyReservesCachesAndRejectsAnotherOwner() throws Exception {
        var definition = store.createDefinition("idem-" + UUID.randomUUID(), "test", json.readTree("""
                {"entry_task":"first","tasks":[{"name":"first","handler_key":"sample.echo"}]}
                """));
        var first = store.createExecutionAndTask(definition.id(), json.readTree("{}"), "first", "sample.echo");
        var second = store.createExecutionAndTask(definition.id(), json.readTree("{}"), "first", "sample.echo");
        var idempotency = new JdbcIdempotencyStore(jdbc, json);
        String key = "test-" + UUID.randomUUID();
        assertFalse(idempotency.beginIdempotentTask("sample.echo", key, first.task().id()).replayed());
        assertFalse(idempotency.beginIdempotentTask("sample.echo", key, first.task().id()).replayed());
        assertThrows(JdbcIdempotencyStore.InProgress.class,
                () -> idempotency.beginIdempotentTask("sample.echo", key, second.task().id()));
        idempotency.completeIdempotentTask("sample.echo", key, first.task().id(), json.readTree("{\"ok\":true}"));
        var cached = idempotency.beginIdempotentTask("sample.echo", key, second.task().id());
        assertTrue(cached.replayed());
        assertEquals(json.readTree("{\"ok\":true}"), cached.response());
        idempotency.releaseIdempotentTask("sample.echo", key, first.task().id());
        assertTrue(idempotency.beginIdempotentTask("sample.echo", key, second.task().id()).replayed());
    }

    @Test
    void workerSchedulesRetryThenCompletesOnSecondDelivery() throws Exception {
        var definition = store.createDefinition("worker-retry-" + UUID.randomUUID(), "test", json.readTree("""
                {"entry_task":"first","tasks":[{"name":"first","handler_key":"test.flaky","max_attempts":2,"backoff_seconds":0}]}
                """));
        var result = store.createExecutionAndTask(definition.id(), json.readTree("{}"), "first", "test.flaky");
        AtomicInteger calls = new AtomicInteger();
        TaskHandler handler = new TaskHandler() {
            @Override public String key() { return "test.flaky"; }
            @Override public com.fasterxml.jackson.databind.JsonNode handle(Models.TaskInstance task) {
                if (calls.incrementAndGet() == 1) throw new IllegalStateException("try again");
                return json.createObjectNode().put("ok", true);
            }
        };
        var worker = new TaskWorker(store, List.of(handler));
        var message = new Models.TaskMessage(result.task().id(), result.execution().id(), "test.flaky");
        worker.handle(message);
        assertEquals("pending", store.getTask(result.task().id()).status());
        assertEquals(1, store.enqueueDueRetries(20));
        worker.handle(message);
        assertEquals("succeeded", store.getTask(result.task().id()).status());
        assertEquals("succeeded", store.getExecution(result.execution().id()).status());
        assertEquals(2, store.getSnapshot(result.execution().id()).tasks().getFirst().attempts().size());
        worker.handle(message);
        assertEquals(2, calls.get());
    }

    @Test
    void redeliveryUsesCachedHandlerResponseAfterInterruptedAttempt() throws Exception {
        var definition = store.createDefinition("cached-" + UUID.randomUUID(), "test", json.readTree("""
                {"entry_task":"first","tasks":[{"name":"first","handler_key":"sample.echo"}]}
                """));
        var result = store.createExecutionAndTask(definition.id(), json.readTree("{\"value\":1}"), "first", "sample.echo");
        var handler = new IdempotentDemoHandler("sample.echo", new JdbcIdempotencyStore(jdbc, json), json);
        var interrupted = store.startTaskAttempt(result.task().id());
        var firstResponse = handler.handle(interrupted.task());
        var worker = new TaskWorker(store, List.of(handler));
        worker.handle(new Models.TaskMessage(result.task().id(), result.execution().id(), "sample.echo"));
        var snapshot = store.getSnapshot(result.execution().id());
        assertEquals("succeeded", snapshot.execution().status());
        assertEquals(firstResponse, snapshot.execution().output());
        assertEquals(2, snapshot.tasks().getFirst().attempts().size());
        assertEquals("running", snapshot.tasks().getFirst().attempts().getFirst().status());
    }

    @Test
    void nextTaskConflictRollsBackAttemptCompletion() throws Exception {
        var definition = store.createDefinition("rollback-" + UUID.randomUUID(), "test", json.readTree("""
                {"entry_task":"first","tasks":[{"name":"first","handler_key":"sample.echo","next_task":"second"},{"name":"second","handler_key":"sample.echo"}]}
                """));
        var result = store.createExecutionAndTask(definition.id(), json.readTree("{}"), "first", "sample.echo");
        var attempt = store.startTaskAttempt(result.task().id());
        jdbc.update("INSERT INTO task_instances (workflow_execution_id, task_name, handler_key, status, input_json, idempotency_key) VALUES (?::uuid, 'second', 'sample.echo', 'pending', '{}'::jsonb, ?)",
                result.execution().id(), result.execution().id() + ":second");
        assertThrows(org.springframework.dao.DuplicateKeyException.class,
                () -> store.completeTaskAttemptAndEnqueueNext(result.task().id(), attempt.attempt().id(), "second", "sample.echo", json.readTree("{}")));
        assertEquals("running", store.getTask(result.task().id()).status());
        assertEquals("running", store.getSnapshot(result.execution().id()).tasks().getFirst().attempts().getFirst().status());
    }

    @Test
    void invalidInputRetriesThenDeadLettersWithoutLeakingReservation() throws Exception {
        var definition = store.createDefinition("invalid-" + UUID.randomUUID(), "test", json.readTree("""
                {"entry_task":"first","tasks":[{"name":"first","handler_key":"sample.echo","max_attempts":2,"backoff_seconds":0}]}
                """));
        var result = store.createExecutionAndTask(definition.id(), json.readTree("123"), "first", "sample.echo");
        var handler = new IdempotentDemoHandler("sample.echo", new JdbcIdempotencyStore(jdbc, json), json);
        var worker = new TaskWorker(store, List.of(handler));
        var message = new Models.TaskMessage(result.task().id(), result.execution().id(), "sample.echo");
        worker.handle(message);
        assertEquals("pending", store.getTask(result.task().id()).status());
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM idempotency_records WHERE idempotency_key = ?", Integer.class, result.task().idempotencyKey()));
        assertEquals(1, store.enqueueDueRetries(20));
        worker.handle(message);
        assertEquals("dead_lettered", store.getTask(result.task().id()).status());
        assertEquals("failed", store.getExecution(result.execution().id()).status());
        assertEquals(2, store.getSnapshot(result.execution().id()).tasks().getFirst().attempts().size());
    }
}
