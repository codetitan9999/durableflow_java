package io.durableflow.telemetry;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.io.StringWriter;

public final class Metrics {
    private Metrics() {}

    public static final Counter HTTP_REQUESTS = Counter.build("durableflow_http_requests_total", "Total HTTP requests served by DurableFlow services.")
            .labelNames("service", "route", "method", "status_code").register();
    public static final Histogram HTTP_DURATION = Histogram.build("durableflow_http_request_duration_seconds", "HTTP request duration by service, route, method, and status code.")
            .labelNames("service", "route", "method", "status_code").register();
    public static final Counter DEFINITIONS = Counter.build("durableflow_workflow_definitions_created_total", "Total workflow definitions created.")
            .labelNames("service").register();
    public static final Counter EXECUTIONS = Counter.build("durableflow_workflow_executions_created_total", "Total workflow executions triggered.")
            .labelNames("service").register();
    public static final Counter DISPATCH = Counter.build("durableflow_dispatch_events_total", "Total task dispatch attempts from the outbox publisher.")
            .labelNames("service", "status").register();
    public static final Counter TASKS = Counter.build("durableflow_tasks_processed_total", "Total task processing attempts by handler and result.")
            .labelNames("service", "handler", "status").register();
    public static final Histogram TASK_DURATION = Histogram.build("durableflow_task_processing_duration_seconds", "Task processing duration by handler and result.")
            .labelNames("service", "handler", "status").register();
    public static final Counter RETRIES_SCHEDULED = Counter.build("durableflow_retries_scheduled_total", "Total task retries scheduled from the worker path.")
            .labelNames("service", "handler").register();
    public static final Counter RETRIES_ENQUEUED = Counter.build("durableflow_retries_enqueued_total", "Total due retries materialized into outbox events.")
            .labelNames("service").register();
    public static final Counter DEAD_LETTERED = Counter.build("durableflow_dead_lettered_tasks_total", "Total tasks moved into dead-letter state.")
            .labelNames("service", "handler", "reason").register();
    public static final Counter REPLAYS = Counter.build("durableflow_task_replays_total", "Total dead-letter replay requests accepted.")
            .labelNames("service").register();
    public static final Counter RECLAIMED = Counter.build("durableflow_reclaimed_messages_total", "Total Redis stream messages reclaimed from the pending list.")
            .labelNames("service", "stream", "group").register();

    public static String scrape() throws IOException {
        StringWriter writer = new StringWriter();
        TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
        return writer.toString();
    }
}
