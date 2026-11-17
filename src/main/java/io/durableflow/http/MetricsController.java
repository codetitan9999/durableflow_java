package io.durableflow.http;

import io.durableflow.telemetry.Metrics;
import java.io.IOException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public final class MetricsController {
    @GetMapping(value = "/metrics", produces = "text/plain; version=0.0.4; charset=utf-8")
    public ResponseEntity<String> metrics() throws IOException {
        return ResponseEntity.ok().contentType(MediaType.parseMediaType("text/plain; version=0.0.4; charset=utf-8")).body(Metrics.scrape());
    }
}
