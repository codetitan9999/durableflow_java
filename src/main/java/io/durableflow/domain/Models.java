package io.durableflow.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.time.Instant;
import java.util.List;

public final class Models {
    private Models() {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public record WorkflowDefinition(String id, String name, String description, int version,
            String status, JsonNode definition, Instant createdAt, Instant updatedAt) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public record WorkflowExecution(String id, String workflowDefinitionId, String status, JsonNode input,
            @JsonInclude(JsonInclude.Include.NON_NULL) JsonNode output,
            @JsonInclude(JsonInclude.Include.NON_EMPTY) String error,
            Instant createdAt, Instant updatedAt,
            @JsonInclude(JsonInclude.Include.NON_NULL) Instant startedAt,
            @JsonInclude(JsonInclude.Include.NON_NULL) Instant completedAt) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public record TaskInstance(String id, String workflowExecutionId, String taskName, String handlerKey,
            String status, JsonNode input,
            @JsonInclude(JsonInclude.Include.NON_NULL) JsonNode output,
            @JsonInclude(JsonInclude.Include.NON_NULL) Instant nextRunAt,
            @JsonInclude(JsonInclude.Include.NON_EMPTY) String lastError,
            int attemptsTotal, String idempotencyKey,
            @JsonInclude(JsonInclude.Include.NON_NULL) Instant dispatchedAt,
            @JsonInclude(JsonInclude.Include.NON_NULL) Instant completedAt,
            Instant createdAt, Instant updatedAt) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public record TaskAttempt(String id, String taskInstanceId, int attemptNumber, String status,
            Instant startedAt, @JsonInclude(JsonInclude.Include.NON_NULL) Instant finishedAt,
            @JsonInclude(JsonInclude.Include.NON_EMPTY) String error,
            @JsonInclude(JsonInclude.Include.NON_NULL) JsonNode output, Instant createdAt) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public record OutboxEvent(String id, String aggregateType, String aggregateId, String eventType,
            JsonNode payload, Instant availableAt,
            @JsonInclude(JsonInclude.Include.NON_NULL) Instant dispatchedAt,
            int attemptCount, @JsonInclude(JsonInclude.Include.NON_EMPTY) String lastError,
            Instant createdAt) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public record TaskMessage(String taskId, String executionId, String handlerKey) {}
    public record ExecutionStartResult(WorkflowExecution execution, TaskInstance task) {}
    public record TaskSnapshot(TaskInstance task, List<TaskAttempt> attempts) {}
    public record ExecutionSnapshot(WorkflowExecution execution, List<TaskSnapshot> tasks) {}
    public record StartedAttempt(TaskInstance task, TaskAttempt attempt, boolean alreadyCompleted) {}
    public record IdempotencyResult(JsonNode response, boolean replayed) {}

    public record WorkflowTaskSpec(String name, String handlerKey, String nextTask, int maxAttempts,
            int backoffSeconds) {}
    public record WorkflowDefinitionSpec(String entryTask, List<WorkflowTaskSpec> tasks) {}
}
