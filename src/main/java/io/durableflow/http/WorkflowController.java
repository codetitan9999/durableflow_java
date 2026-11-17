package io.durableflow.http;

import com.fasterxml.jackson.databind.JsonNode;
import io.durableflow.orchestrator.WorkflowService;
import io.durableflow.persistence.StoreExceptions;
import io.durableflow.telemetry.Metrics;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnProperty(name = "durableflow.role", havingValue = "api", matchIfMissing = true)
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class WorkflowController {
    private final WorkflowService workflows;

    public WorkflowController(WorkflowService workflows) { this.workflows = workflows; }

    @PostMapping("/workflows")
    public ResponseEntity<?> createWorkflow(@RequestBody JsonNode body) {
        var result = workflows.createWorkflow(text(body, "name"), text(body, "description"), body.get("definition"));
        Metrics.DEFINITIONS.labels("api").inc();
        return ResponseEntity.status(HttpStatus.CREATED).body(result);
    }

    @PostMapping("/executions")
    public ResponseEntity<?> trigger(@RequestBody JsonNode body) {
        try {
            var result = workflows.trigger(text(body, "workflow_definition_id"), body.get("input"));
            Metrics.EXECUTIONS.labels("api").inc();
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(result);
        } catch (StoreExceptions.NotFound error) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "workflow definition not found"));
        }
    }

    @GetMapping("/executions/{id}")
    public ResponseEntity<?> snapshot(@PathVariable String id) {
        try {
            return ResponseEntity.ok(workflows.snapshot(id));
        } catch (StoreExceptions.NotFound error) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "execution not found"));
        }
    }

    @GetMapping({"/dead-letter-tasks", "/dead-lettered-tasks"})
    public ResponseEntity<?> deadLettered(@RequestParam(required = false) String limit) {
        int parsed = 0;
        if (limit != null && !limit.isBlank()) {
            try { parsed = Integer.parseInt(limit.strip()); }
            catch (NumberFormatException error) { return ResponseEntity.badRequest().body(Map.of("error", "limit must be a valid integer")); }
            if (parsed <= 0) return ResponseEntity.badRequest().body(Map.of("error", "limit must be greater than 0"));
        }
        var tasks = workflows.deadLettered(parsed);
        return ResponseEntity.ok(tasks.isEmpty() ? com.fasterxml.jackson.databind.node.NullNode.instance : tasks);
    }

    @PostMapping("/tasks/{id}/replay")
    public ResponseEntity<?> replay(@PathVariable String id) {
        try {
            var result = workflows.replay(id);
            Metrics.REPLAYS.labels("api").inc();
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(result);
        } catch (StoreExceptions.NotFound error) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "task not found"));
        } catch (StoreExceptions.NotReplayable error) {
            return ResponseEntity.badRequest().body(Map.of("error", "only dead-lettered tasks can be replayed"));
        }
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<?> badRequest(IllegalArgumentException error) {
        return ResponseEntity.badRequest().body(Map.of("error", error.getMessage()));
    }

    private static String text(JsonNode body, String key) {
        JsonNode value = body.get(key);
        return value == null || value.isNull() ? "" : value.asText();
    }
}
