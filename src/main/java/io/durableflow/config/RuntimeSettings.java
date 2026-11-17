package io.durableflow.config;

import java.time.Duration;
import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record RuntimeSettings(String role, String redisAddress, String stream, String group, String consumer,
                              Duration reclaimMinIdle, long reclaimCount, Duration outboxPollInterval, Duration startupTimeout) {
    private static final Pattern DURATION = Pattern.compile("((?:\\d+(?:\\.\\d*)?|\\.\\d+))(ns|us|µs|μs|ms|s|m|h)");

    public static RuntimeSettings fromEnvironment() {
        String role = env("DURABLEFLOW_ROLE", "api");
        if (!role.equals("api") && !role.equals("worker")) throw new IllegalArgumentException("DURABLEFLOW_ROLE must be api or worker");
        long reclaimCount = Long.parseLong(env("REDIS_RECLAIM_COUNT", "10"));
        if (reclaimCount <= 0) throw new IllegalArgumentException("REDIS_RECLAIM_COUNT must be greater than 0");
        return new RuntimeSettings(role, required("REDIS_ADDR"), required("REDIS_STREAM"),
                required("REDIS_GROUP"), env("REDIS_CONSUMER", role.equals("api") ? "api" : "worker-" + System.currentTimeMillis() / 1000),
                duration(env("REDIS_RECLAIM_MIN_IDLE", "30s")), reclaimCount,
                duration(env("OUTBOX_POLL_INTERVAL", "2s")), duration(env("STARTUP_TIMEOUT", "30s")));
    }

    public static Duration duration(String text) {
        String normalized = text.startsWith("+") ? text.substring(1) : text;
        Matcher matcher = DURATION.matcher(normalized);
        BigDecimal nanos = BigDecimal.ZERO;
        int end = 0;
        while (matcher.find() && matcher.start() == end) {
            BigDecimal unit = switch (matcher.group(2)) {
                case "ns" -> BigDecimal.ONE;
                case "us", "µs", "μs" -> BigDecimal.valueOf(1_000);
                case "ms" -> BigDecimal.valueOf(1_000_000);
                case "s" -> BigDecimal.valueOf(1_000_000_000);
                case "m" -> BigDecimal.valueOf(60_000_000_000L);
                case "h" -> BigDecimal.valueOf(3_600_000_000_000L);
                default -> throw new IllegalArgumentException("Invalid duration");
            };
            nanos = nanos.add(new BigDecimal(matcher.group(1)).multiply(unit));
            end = matcher.end();
        }
        if (end != normalized.length() || nanos.signum() <= 0) throw new IllegalArgumentException("Invalid positive duration: " + text);
        return Duration.ofNanos(nanos.longValueExact());
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static String required(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException(key + " must not be empty");
        return value;
    }
}
