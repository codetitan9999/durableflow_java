package io.durableflow.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.domain.Models;
import io.durableflow.domain.WorkflowDefinitions;
import io.durableflow.persistence.JdbcWorkflowStore;
import java.util.List;
import org.springframework.dao.DuplicateKeyException;

public final class WorkflowService {
    private final JdbcWorkflowStore store;
    private final ObjectMapper json;

    public WorkflowService(JdbcWorkflowStore store, ObjectMapper json) {
        this.store = store;
        this.json = json;
    }

    public Models.WorkflowDefinition createWorkflow(String name, String description, JsonNode definition) {
        String trimmed = name == null ? "" : name.strip();
        if (trimmed.isEmpty()) throw new IllegalArgumentException("workflow name is required");
        JsonNode actual = definition;
        if (actual == null || actual.isNull()) {
            actual = json.createObjectNode().put("entry_task", "sample-task")
                    .set("tasks", json.createArrayNode().add(json.createObjectNode().put("name", "sample-task").put("handler_key", "sample.echo")));
        }
        WorkflowDefinitions.parse(actual);
        try {
            return store.createDefinition(trimmed, description == null ? "" : description, actual);
        } catch (DuplicateKeyException error) {
            throw new IllegalArgumentException("workflow name already exists", error);
        }
    }

    public Models.ExecutionStartResult trigger(String definitionId, JsonNode input) {
        if (definitionId == null || definitionId.isBlank()) throw new IllegalArgumentException("workflow_definition_id is required");
        var definition = store.getDefinition(definitionId);
        var entry = WorkflowDefinitions.entry(WorkflowDefinitions.parse(definition.definition()));
        return store.createExecutionAndTask(definitionId, input, entry.name(), entry.handlerKey());
    }

    public Models.ExecutionSnapshot snapshot(String id) { return store.getSnapshot(id); }

    public List<Models.TaskInstance> deadLettered(int limit) {
        return store.listDeadLettered(WorkflowDefinitions.deadLetterLimit(limit));
    }

    public Models.TaskInstance replay(String taskId) { return store.replayDeadLettered(taskId); }
}
