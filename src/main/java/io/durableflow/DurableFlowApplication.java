package io.durableflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.orchestrator.WorkflowService;
import io.durableflow.orchestrator.TaskWorker;
import io.durableflow.handlers.TaskHandler;
import io.durableflow.handlers.IdempotentDemoHandler;
import io.durableflow.config.RuntimeSettings;
import io.durableflow.outbox.OutboxPublisher;
import io.durableflow.persistence.JdbcIdempotencyStore;
import io.durableflow.queue.RedisStreams;
import java.util.List;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import io.durableflow.persistence.JdbcWorkflowStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootApplication
public class DurableFlowApplication {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DurableFlowApplication.class);
    public static void main(String[] args) {
        String role = System.getenv().getOrDefault("DURABLEFLOW_ROLE", "api");
        String address = System.getenv().getOrDefault(role.equals("worker") ? "WORKER_HTTP_ADDR" : "API_HTTP_ADDR",
                role.equals("worker") ? ":8081" : ":8080");
        System.setProperty("server.port", address.substring(address.lastIndexOf(':') + 1));
        System.setProperty("durableflow.role", role);
        SpringApplication.run(DurableFlowApplication.class, args);
    }

    @Bean
    RuntimeSettings runtimeSettings() { return RuntimeSettings.fromEnvironment(); }

    @Bean(destroyMethod = "close")
    RedisStreams redisStreams(RuntimeSettings settings, ObjectMapper json) {
        java.time.Instant deadline = java.time.Instant.now().plus(settings.startupTimeout());
        RuntimeException last = null;
        while (java.time.Instant.now().isBefore(deadline)) {
            try {
                var redis = new RedisStreams(settings.redisAddress(), settings.stream(), settings.group(), json);
                redis.ping();
                return redis;
            } catch (RuntimeException error) {
                last = error;
                try { Thread.sleep(1000); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for Redis", interrupted);
                }
            }
        }
        throw new IllegalStateException("Redis unavailable before startup timeout", last);
    }

    @Bean
    JdbcIdempotencyStore idempotencyStore(JdbcTemplate jdbc, ObjectMapper json) {
        return new JdbcIdempotencyStore(jdbc, json);
    }

    @Bean
    TaskWorker taskWorker(JdbcWorkflowStore store, JdbcIdempotencyStore idempotency, ObjectMapper json) {
        List<TaskHandler> handlers = List.of(new IdempotentDemoHandler("sample.echo", idempotency, json),
                new IdempotentDemoHandler("notifications.send", idempotency, json));
        return new TaskWorker(store, handlers);
    }

    @Bean
    OutboxPublisher outboxPublisher(JdbcWorkflowStore store, RedisStreams redis, ObjectMapper json) {
        return new OutboxPublisher(store, redis, json);
    }

    @Bean
    @Order(1)
    ApplicationRunner runtimeRunner(RuntimeSettings settings, RedisStreams redis, TaskWorker worker,
                                    OutboxPublisher publisher, ConfigurableApplicationContext context) {
        return args -> {
            if (settings.role().equals("api")) {
                Thread.ofVirtual().name("outbox-publisher").start(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            publisher.publishOnce();
                        } catch (RuntimeException error) {
                            LOG.error("outbox publish batch failed", error);
                        }
                        try { Thread.sleep(settings.outboxPollInterval()); }
                        catch (InterruptedException error) { Thread.currentThread().interrupt(); }
                    }
                });
            } else {
                Thread.ofVirtual().name("redis-consumer").start(() -> {
                    try {
                        redis.consume(settings.consumer(), settings.reclaimMinIdle(), settings.reclaimCount(), worker::handle);
                    } catch (RuntimeException error) {
                        LOG.error("stream consumer exited", error);
                        context.close();
                    }
                });
            }
        };
    }

    @Bean
    JdbcWorkflowStore workflowStore(JdbcTemplate jdbc, PlatformTransactionManager transactionManager, ObjectMapper json) {
        return new JdbcWorkflowStore(jdbc, new TransactionTemplate(transactionManager), json);
    }

    @Bean
    WorkflowService workflowService(JdbcWorkflowStore store, ObjectMapper json) {
        return new WorkflowService(store, json);
    }
}
