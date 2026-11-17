package io.durableflow.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import io.durableflow.domain.Models.IdempotencyResult;

public interface IdempotencyStore {
    IdempotencyResult beginIdempotentTask(String handlerKey, String key, String ownerTaskId);
    void completeIdempotentTask(String handlerKey, String key, String ownerTaskId, JsonNode response);
    void releaseIdempotentTask(String handlerKey, String key, String ownerTaskId);
}
