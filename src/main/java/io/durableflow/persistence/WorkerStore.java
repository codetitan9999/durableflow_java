package io.durableflow.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import static io.durableflow.domain.Models.*;

public interface WorkerStore {
    TaskInstance getTaskInstance(String id);
    WorkflowExecution getWorkflowExecution(String id);
    WorkflowDefinition getWorkflowDefinition(String id);
    StartedAttempt startTaskAttempt(String taskId);
    void scheduleTaskRetry(String taskId, String attemptId, String error, Instant nextRunAt);
    void failTaskAttempt(String taskId, String attemptId, String error);
    void completeTaskAttempt(String taskId, String attemptId, JsonNode output);
    void completeTaskAttemptAndEnqueueNextTask(String taskId, String attemptId, String name, String handlerKey, JsonNode output);
}
