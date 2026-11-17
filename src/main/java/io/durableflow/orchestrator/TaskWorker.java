package io.durableflow.orchestrator;

import io.durableflow.domain.Models;
import io.durableflow.domain.WorkflowDefinitions;
import io.durableflow.handlers.TaskHandler;
import io.durableflow.persistence.JdbcWorkflowStore;
import io.durableflow.telemetry.Metrics;
import java.time.Instant;
import java.util.Map;

public final class TaskWorker {
    private final JdbcWorkflowStore store;
    private final Map<String, TaskHandler> handlers;

    public TaskWorker(JdbcWorkflowStore store, Iterable<TaskHandler> handlers) {
        this.store = store;
        var registry = new java.util.HashMap<String, TaskHandler>();
        for (var handler : handlers) {
            if (registry.putIfAbsent(handler.key(), handler) != null) throw new IllegalArgumentException("duplicate handler: " + handler.key());
        }
        this.handlers = Map.copyOf(registry);
    }

    public void handle(Models.TaskMessage message) {
        long startedAt = System.nanoTime();
        String outcome = "db_error";
        try {
            outcome = process(message);
        } finally {
            Metrics.TASKS.labels("worker", message.handlerKey(), outcome).inc();
            Metrics.TASK_DURATION.labels("worker", message.handlerKey(), outcome).observe((System.nanoTime() - startedAt) / 1_000_000_000.0);
        }
    }

    private String process(Models.TaskMessage message) {
        var existing = store.getTask(message.taskId());
        var execution = store.getExecution(existing.workflowExecutionId());
        var definition = store.getDefinition(execution.workflowDefinitionId());
        var workflow = WorkflowDefinitions.parse(definition.definition());
        var spec = WorkflowDefinitions.find(workflow, existing.taskName());
        int maxAttempts = spec.maxAttempts() == 0 ? 1 : spec.maxAttempts();

        var started = store.startTaskAttempt(message.taskId());
        if (started.alreadyCompleted()) return "duplicate";
        var task = started.task();
        var attempt = started.attempt();
        var handler = handlers.get(task.handlerKey());
        if (handler == null) {
            store.failTaskAttempt(task.id(), attempt.id(), "no handler registered for \"" + task.handlerKey() + "\"");
            Metrics.DEAD_LETTERED.labels("worker", message.handlerKey(), "missing_handler").inc();
            return "missing_handler";
        }

        final com.fasterxml.jackson.databind.JsonNode output;
        try {
            output = handler.handle(task);
        } catch (Exception error) {
            if (maxAttempts > attempt.attemptNumber()) {
                store.scheduleTaskRetry(task.id(), attempt.id(), error.getMessage(), Instant.now().plusSeconds(spec.backoffSeconds()));
                Metrics.RETRIES_SCHEDULED.labels("worker", message.handlerKey()).inc();
                return "scheduled_retry";
            } else {
                store.failTaskAttempt(task.id(), attempt.id(), error.getMessage());
                Metrics.DEAD_LETTERED.labels("worker", message.handlerKey(), "exhausted_attempts").inc();
                return "failed";
            }
        }

        var next = WorkflowDefinitions.next(workflow, task.taskName());
        if (next == null) {
            store.completeTaskAttempt(task.id(), attempt.id(), output);
            return "succeeded";
        }
        store.completeTaskAttemptAndEnqueueNext(task.id(), attempt.id(), next.name(), next.handlerKey(), output);
        return "advanced";
    }
}
