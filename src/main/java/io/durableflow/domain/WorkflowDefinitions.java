package io.durableflow.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import static io.durableflow.domain.Models.*;

public final class WorkflowDefinitions {
    private WorkflowDefinitions() {}

    public static WorkflowDefinitionSpec parse(JsonNode json) {
        if (json != null && !json.isNull() && !json.isObject()) {
            throw new IllegalArgumentException("unmarshal workflow definition: expected an object");
        }
        String entry = string(json, "entry_task");
        JsonNode taskNodes = json == null ? null : json.get("tasks");
        if (taskNodes != null && !taskNodes.isNull() && !taskNodes.isArray()) {
            throw new IllegalArgumentException("unmarshal workflow definition: tasks must be an array");
        }
        var tasks = new ArrayList<WorkflowTaskSpec>();
        if (taskNodes != null && taskNodes.isArray()) {
            for (JsonNode task : taskNodes) {
                if (!task.isNull() && !task.isObject()) {
                    throw new IllegalArgumentException("unmarshal workflow definition: task must be an object");
                }
                tasks.add(new WorkflowTaskSpec(string(task, "name"), string(task, "handler_key"),
                        string(task, "next_task"), integer(task, "max_attempts"), integer(task, "backoff_seconds")));
            }
        }
        if (entry.strip().isEmpty()) fail("workflow definition must have an entry_task");
        if (tasks.isEmpty()) fail("workflow definition must have at least one task");
        var spec = new WorkflowDefinitionSpec(entry, List.copyOf(tasks));
        boolean found = false;
        for (var task : tasks) {
            if (task.name().strip().isEmpty()) fail("workflow definition tasks must have a name");
            if (task.handlerKey().strip().isEmpty()) fail("workflow definition tasks must have a handler_key");
            if (task.name().strip().equals(entry.strip())) found = true;
            if (task.maxAttempts() < 0) fail("workflow definition tasks must have max_attempts of at least 0");
            if (task.backoffSeconds() < 0) fail("workflow definition tasks must have backoff_seconds of at least 0");
            if (!task.nextTask().strip().isEmpty()) {
                if (task.nextTask().strip().equals(task.name().strip())) fail("workflow definition tasks cannot have next_task that points to itself");
                try { find(spec, task.nextTask()); }
                catch (IllegalArgumentException e) { throw new IllegalArgumentException("workflow definition tasks have invalid next_task: " + e.getMessage(), e); }
            }
        }
        if (!found) fail("entry_task must be one of the tasks defined in the workflow definition");
        return spec;
    }

    public static WorkflowTaskSpec find(WorkflowDefinitionSpec spec, String name) {
        return spec.tasks().stream().filter(t -> t.name().strip().equals(name.strip())).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("task not found in workflow definition: " + name));
    }

    public static WorkflowTaskSpec entry(WorkflowDefinitionSpec spec) {
        return spec.tasks().stream().filter(t -> t.name().strip().equals(spec.entryTask().strip())).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("entry_task must be one of the tasks defined in the workflow definition"));
    }

    public static WorkflowTaskSpec next(WorkflowDefinitionSpec spec, String name) {
        WorkflowTaskSpec current;
        try { current = find(spec, name); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("failed to find current task: " + e.getMessage(), e); }
        if (current.nextTask().strip().isEmpty()) return null;
        try { return find(spec, current.nextTask()); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("invalid next_task reference: " + e.getMessage(), e); }
    }

    public static int deadLetterLimit(int requested) {
        return requested <= 0 ? 100 : Math.min(requested, 500);
    }

    public static String string(JsonNode object, String field) {
        JsonNode value = object == null ? null : object.get(field);
        if (value == null || value.isNull()) return "";
        if (!value.isTextual()) throw new IllegalArgumentException("unmarshal workflow definition: " + field + " must be a string");
        return value.textValue();
    }

    private static int integer(JsonNode object, String field) {
        JsonNode value = object == null ? null : object.get(field);
        if (value == null || value.isNull()) return 0;
        if (!value.isIntegralNumber() || !value.canConvertToInt()) throw new IllegalArgumentException("unmarshal workflow definition: " + field + " must be an integer");
        return value.intValue();
    }

    private static void fail(String message) { throw new IllegalArgumentException(message); }
}
