package io.durableflow.http;

import io.durableflow.config.RuntimeSettings;
import io.durableflow.telemetry.Metrics;
import io.opentelemetry.api.trace.Tracer;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public final class TelemetryFilter extends OncePerRequestFilter {
    private final RuntimeSettings settings;
    private final Tracer tracer;

    public TelemetryFilter(RuntimeSettings settings, Tracer tracer) {
        this.settings = settings;
        this.tracer = tracer;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        long started = System.nanoTime();
        var span = tracer.spanBuilder(request.getRequestURI()).startSpan();
        try (var scope = span.makeCurrent()) {
            chain.doFilter(request, response);
        } finally {
            span.end();
        }
        String service = System.getenv().getOrDefault("SERVICE_NAME", "durableflow-" + settings.role());
        String route = request.getRequestURI();
        String method = request.getMethod();
        String status = Integer.toString(response.getStatus());
        Metrics.HTTP_REQUESTS.labels(service, route, method, status).inc();
        Metrics.HTTP_DURATION.labels(service, route, method, status).observe((System.nanoTime() - started) / 1_000_000_000.0);
    }
}
