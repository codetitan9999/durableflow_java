package io.durableflow.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.domain.Models;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XReadArgs;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class RedisStreamsIntegrationTest {
    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Test
    void reclaimsAbandonedMessageAndAcknowledgesIt() throws Exception {
        String address = redis.getHost() + ":" + redis.getMappedPort(6379);
        String stream = "test.tasks";
        String group = "test-workers";
        try (var queue = new RedisStreams(address, stream, group, new ObjectMapper())) {
            queue.ensureGroup();
            var message = new Models.TaskMessage("task-1", "execution-1", "sample.echo");
            queue.dispatch(message);
            RedisClient client = RedisClient.create("redis://" + address);
            try (var connection = client.connect()) {
                var pending = connection.sync().xreadgroup(Consumer.from(group, "abandoned"),
                        XReadArgs.StreamOffset.lastConsumed(stream));
                assertEquals(1, pending.size());
                Thread.sleep(30);
                CountDownLatch delivered = new CountDownLatch(1);
                AtomicReference<Models.TaskMessage> received = new AtomicReference<>();
                Thread consumer = Thread.ofVirtual().start(() -> queue.consume("replacement", Duration.ofMillis(10), 10, value -> {
                    received.set(value);
                    delivered.countDown();
                }));
                try {
                    assertTrue(delivered.await(5, TimeUnit.SECONDS));
                    assertEquals(message, received.get());
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                    while (connection.sync().xpending(stream, group).getCount() != 0 && System.nanoTime() < deadline) {
                        Thread.sleep(10);
                    }
                    assertEquals(0, connection.sync().xpending(stream, group).getCount());
                } finally {
                    consumer.interrupt();
                }
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void invalidPayloadExitsConsumerWithoutAcknowledging() {
        String address = redis.getHost() + ":" + redis.getMappedPort(6379);
        String stream = "invalid.tasks";
        String group = "invalid-workers";
        try (var queue = new RedisStreams(address, stream, group, new ObjectMapper())) {
            queue.ensureGroup();
            RedisClient client = RedisClient.create("redis://" + address);
            try (var connection = client.connect()) {
                connection.sync().xadd(stream, java.util.Map.of("wrong_field", "{}"));
                assertThrows(IllegalArgumentException.class,
                        () -> queue.consume("consumer", Duration.ofMillis(10), 10, ignored -> fail("handler must not run")));
                assertEquals(1, connection.sync().xpending(stream, group).getCount());
            } finally {
                client.shutdown();
            }
        }
    }
}
