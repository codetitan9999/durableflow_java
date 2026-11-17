package io.durableflow.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.domain.Models;
import io.durableflow.handlers.IdempotentDemoHandler;
import io.durableflow.orchestrator.TaskWorker;
import io.durableflow.persistence.JdbcIdempotencyStore;
import io.durableflow.persistence.JdbcWorkflowStore;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XReadArgs;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class WorkerCrashRecoveryIntegrationTest {
    @Container static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");
    @Container static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Test
    void abandonedMessageReusesCachedResponseAndIsAcknowledged() throws Exception {
        ObjectMapper json = new ObjectMapper();
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        for (String file : List.of("001_init.sql", "002_idempotency_records.sql", "003_idempotency_record_ownership.sql")) {
            jdbc.execute(Files.readString(Path.of("migrations", file)));
        }
        JdbcWorkflowStore store = new JdbcWorkflowStore(jdbc, new TransactionTemplate(new DataSourceTransactionManager(dataSource)), json);
        var definition = store.createDefinition("crash-" + UUID.randomUUID(), "test", json.readTree("""
                {"entry_task":"first","tasks":[{"name":"first","handler_key":"sample.echo"}]}
                """));
        var execution = store.createExecutionAndTask(definition.id(), json.readTree("{\"value\":1}"), "first", "sample.echo");
        String address = redis.getHost() + ":" + redis.getMappedPort(6379);
        String stream = "crash.tasks";
        String group = "crash-workers";
        try (var queue = new RedisStreams(address, stream, group, json)) {
            queue.ensureGroup();
            var message = new Models.TaskMessage(execution.task().id(), execution.execution().id(), "sample.echo");
            queue.dispatch(message);
            RedisClient client = RedisClient.create("redis://" + address);
            try (var connection = client.connect()) {
                var pending = connection.sync().xreadgroup(Consumer.from(group, "crashed-worker"),
                        XReadArgs.StreamOffset.lastConsumed(stream));
                assertEquals(1, pending.size());
                var firstAttempt = store.startTaskAttempt(execution.task().id());
                var handler = new IdempotentDemoHandler("sample.echo", new JdbcIdempotencyStore(jdbc, json), json);
                var cached = handler.handle(firstAttempt.task());
                assertEquals("running", store.getTask(execution.task().id()).status());
                Thread.sleep(30);

                CountDownLatch processed = new CountDownLatch(1);
                TaskWorker worker = new TaskWorker(store, List.of(handler));
                Thread replacement = Thread.ofVirtual().start(() -> queue.consume("replacement-worker", Duration.ofMillis(10), 10, item -> {
                    worker.handle(item);
                    processed.countDown();
                }));
                try {
                    assertTrue(processed.await(5, TimeUnit.SECONDS));
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                    while (connection.sync().xpending(stream, group).getCount() != 0 && System.nanoTime() < deadline) Thread.sleep(10);
                    assertEquals(0, connection.sync().xpending(stream, group).getCount());
                    assertEquals("succeeded", store.getExecution(execution.execution().id()).status());
                    assertEquals(cached, store.getExecution(execution.execution().id()).output());
                    assertEquals(2, store.getSnapshot(execution.execution().id()).tasks().getFirst().attempts().size());
                } finally {
                    replacement.interrupt();
                }
            } finally {
                client.shutdown();
            }
        }
    }
}
