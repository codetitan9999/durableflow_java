package io.durableflow.http;

import java.util.Map;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import io.durableflow.config.RuntimeSettings;
import io.durableflow.queue.RedisStreams;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {
    private final JdbcTemplate jdbc;
    private final RedisStreams redis;
    private final RuntimeSettings settings;

    public HealthController(JdbcTemplate jdbc, RedisStreams redis, RuntimeSettings settings) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.settings = settings;
    }

    @GetMapping("/healthz")
    public ResponseEntity<?> health() {
        try {
            jdbc.queryForObject("SELECT 1", Integer.class);
            redis.ping();
            return ResponseEntity.ok(Map.of("service", settings.role(), "status", "ok"));
        } catch (RuntimeException error) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("service", settings.role(), "status", "unhealthy", "error", error.getMessage()));
        }
    }
}
