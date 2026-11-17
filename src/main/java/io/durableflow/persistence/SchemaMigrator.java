package io.durableflow.persistence;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.core.annotation.Order;
import io.durableflow.config.RuntimeSettings;
import java.time.Instant;

@Component
@Order(0)
public final class SchemaMigrator implements ApplicationRunner {
    private static final long LOCK_ID = 7281459218345L;
    private static final List<String> FILES = List.of("001_init.sql", "002_idempotency_records.sql", "003_idempotency_record_ownership.sql");
    private final DataSource dataSource;
    private final RuntimeSettings settings;

    public SchemaMigrator(DataSource dataSource, RuntimeSettings settings) {
        this.dataSource = dataSource;
        this.settings = settings;
    }

    @Override
    public void run(ApplicationArguments args) throws SQLException, IOException {
        try (Connection connection = waitForDatabase()) {
            try (var lock = connection.prepareStatement("SELECT pg_advisory_lock(?)")) {
                lock.setLong(1, LOCK_ID);
                lock.execute();
            }
            try {
                try (var statement = connection.createStatement()) {
                    statement.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())");
                }
                for (String file : FILES) {
                    boolean applied;
                    try (var check = connection.prepareStatement("SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = ?)")) {
                        check.setString(1, file);
                        try (var rows = check.executeQuery()) { rows.next(); applied = rows.getBoolean(1); }
                    }
                    if (applied) continue;
                    String sql = new ClassPathResource("migrations/" + file).getContentAsString(StandardCharsets.UTF_8);
                    connection.setAutoCommit(false);
                    try {
                        try (var statement = connection.createStatement()) { statement.execute(sql); }
                        try (var record = connection.prepareStatement("INSERT INTO schema_migrations (version) VALUES (?) ON CONFLICT (version) DO NOTHING")) {
                            record.setString(1, file);
                            record.executeUpdate();
                        }
                        connection.commit();
                    } catch (SQLException error) {
                        connection.rollback();
                        throw error;
                    } finally {
                        connection.setAutoCommit(true);
                    }
                }
            } finally {
                try (var unlock = connection.prepareStatement("SELECT pg_advisory_unlock(?)")) {
                    unlock.setLong(1, LOCK_ID);
                    unlock.execute();
                }
            }
        }
    }

    private Connection waitForDatabase() throws SQLException {
        Instant deadline = Instant.now().plus(settings.startupTimeout());
        SQLException last = null;
        while (Instant.now().isBefore(deadline)) {
            try { return dataSource.getConnection(); }
            catch (SQLException error) { last = error; }
            try { Thread.sleep(1000); }
            catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while waiting for PostgreSQL", error);
            }
        }
        throw new SQLException("PostgreSQL unavailable before startup timeout", last);
    }
}
