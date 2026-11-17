package io.durableflow.persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatabaseConfiguration {
    @Bean
    DataSource dataSource() {
        String raw = System.getenv("DATABASE_URL");
        if (raw == null || raw.isBlank()) throw new IllegalArgumentException("DATABASE_URL must not be empty");
        URI uri = URI.create(raw);
        if (!"postgres".equals(uri.getScheme()) && !"postgresql".equals(uri.getScheme())) {
            throw new IllegalArgumentException("DATABASE_URL must use postgres:// or postgresql://");
        }
        String[] credentials = uri.getUserInfo() == null ? new String[0] : uri.getUserInfo().split(":", 2);
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + uri.getHost() + (uri.getPort() < 0 ? "" : ":" + uri.getPort()) + uri.getPath()
                + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery()));
        config.setUsername(credentials.length > 0 ? credentials[0] : "postgres");
        config.setPassword(credentials.length > 1 ? credentials[1] : "");
        config.setMaximumPoolSize(8);
        return new HikariDataSource(config);
    }
}
