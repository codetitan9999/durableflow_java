package io.durableflow.config;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RuntimeSettingsTest {
    @Test
    void parsesGoStyleDurations() {
        assertEquals(Duration.ofMillis(100), RuntimeSettings.duration("100ms"));
        assertEquals(Duration.ofMillis(1500), RuntimeSettings.duration("1.5s"));
        assertEquals(Duration.ofMinutes(90), RuntimeSettings.duration("1h30m"));
        assertEquals(Duration.ofNanos(250_000), RuntimeSettings.duration("250us"));
        assertEquals(Duration.ofNanos(1), RuntimeSettings.duration("1ns"));
        assertEquals(Duration.ofSeconds(1), RuntimeSettings.duration("+1s"));
    }

    @Test
    void rejectsZeroAndMalformedDurations() {
        assertThrows(IllegalArgumentException.class, () -> RuntimeSettings.duration("0s"));
        assertThrows(IllegalArgumentException.class, () -> RuntimeSettings.duration("-1s"));
        assertThrows(IllegalArgumentException.class, () -> RuntimeSettings.duration("1day"));
    }
}
