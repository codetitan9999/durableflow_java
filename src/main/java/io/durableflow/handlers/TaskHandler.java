package io.durableflow.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import io.durableflow.domain.Models;

public interface TaskHandler {
    String key();
    JsonNode handle(Models.TaskInstance task);
}
