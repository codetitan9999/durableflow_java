package io.durableflow.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.domain.Models;
import io.durableflow.telemetry.Metrics;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisCommandInterruptedException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public final class RedisStreams implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(RedisStreams.class);
    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final ObjectMapper json;
    private final String stream;
    private final String group;

    public RedisStreams(String address, String stream, String group, ObjectMapper json) {
        this.client = RedisClient.create("redis://" + address);
        try {
            this.connection = client.connect();
        } catch (RuntimeException error) {
            client.shutdown();
            throw error;
        }
        this.json = json;
        this.stream = stream;
        this.group = group;
    }

    public void ping() { connection.sync().ping(); }

    public void ensureGroup() {
        try {
            connection.sync().xgroupCreate(XReadArgs.StreamOffset.from(stream, "0"), group, new XGroupCreateArgs().mkstream(true));
        } catch (RedisBusyException ignored) {
            // Redis returns BUSYGROUP when another process created the group.
        }
    }

    public void dispatch(Models.TaskMessage message) {
        try {
            connection.sync().xadd(stream, Map.of("payload", json.writeValueAsString(message)));
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("Invalid task message", error);
        }
    }

    public void consume(String consumerName, Duration minIdle, long reclaimCount, java.util.function.Consumer<Models.TaskMessage> handler) {
        ensureGroup();
        var consumer = Consumer.from(group, consumerName);
        while (!Thread.currentThread().isInterrupted()) {
            var reclaimed = connection.sync().xautoclaim(stream, new XAutoClaimArgs<String>().consumer(consumer)
                    .minIdleTime(minIdle).startId("0-0").count(reclaimCount)).getMessages();
            if (!reclaimed.isEmpty()) {
                Metrics.RECLAIMED.labels("worker", stream, group).inc(reclaimed.size());
                process(reclaimed, handler);
                continue;
            }
            List<StreamMessage<String, String>> fresh;
            try {
                fresh = connection.sync().xreadgroup(consumer, new XReadArgs().count(1).block(Duration.ofSeconds(5)),
                        XReadArgs.StreamOffset.lastConsumed(stream));
            } catch (RedisCommandInterruptedException error) {
                if (Thread.currentThread().isInterrupted()) return;
                throw error;
            }
            if (fresh != null) process(fresh, handler);
        }
    }

    private void process(List<StreamMessage<String, String>> records, java.util.function.Consumer<Models.TaskMessage> handler) {
        for (var record : records) {
            String payload = record.getBody().get("payload");
            if (payload == null) throw new IllegalArgumentException("stream message " + record.getId() + " missing payload");
            final Models.TaskMessage message;
            try { message = json.readValue(payload, Models.TaskMessage.class); }
            catch (JsonProcessingException error) { throw new IllegalArgumentException("decode stream message " + record.getId(), error); }
            try { handler.accept(message); }
            catch (RuntimeException error) {
                LOG.error("task processing failed: task_id={} stream_message_id={}", message.taskId(), record.getId(), error);
                continue;
            }
            connection.sync().xack(stream, group, record.getId());
        }
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown();
    }
}
