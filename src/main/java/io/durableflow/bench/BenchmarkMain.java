package io.durableflow.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.durableflow.config.RuntimeSettings;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class BenchmarkMain {
    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private String baseUrl;
    private Duration pollInterval;
    private Duration timeout;

    public static void main(String[] args) throws Exception { new BenchmarkMain().run(args); }

    private void run(String[] args) throws Exception {
        Map<String, String> options = parseOptions(args);
        baseUrl = options.getOrDefault("api-base-url", "http://localhost:8080").replaceAll("/+$", "");
        pollInterval = RuntimeSettings.duration(options.getOrDefault("poll-interval", "200ms"));
        timeout = RuntimeSettings.duration(options.getOrDefault("timeout", "30s"));
        String scenario = options.getOrDefault("scenario", "success-linear");
        int executions = positive(options, "executions", 25);
        int concurrency = positive(options, "concurrency", 5);
        int repeat = positive(options, "repeat", 1);
        int attempts = positive(options, "max-attempts", 3);
        int backoff = nonNegative(options, "backoff-seconds", 1);
        int chain = positive(options, "chain-length", 5);
        if (chain < 2) throw new IllegalArgumentException("chain-length must be at least 2");
        ObjectNode spec = scenario(scenario, attempts, backoff, chain);
        ArrayNode results = json.createArrayNode();
        List<Double> throughput = new ArrayList<>();
        List<Long> elapsed = new ArrayList<>();
        List<Long> p95 = new ArrayList<>();
        List<Long> avg = new ArrayList<>();
        List<Long> observedP95 = new ArrayList<>();
        int succeededRuns = 0;
        for (int repetition = 0; repetition < repeat; repetition++) {
            String name = "bench-" + scenario + "-" + System.nanoTime() + "-" + repetition;
            ObjectNode create = json.createObjectNode().put("name", name).put("description", "Java benchmark: " + scenario);
            create.set("definition", spec);
            String workflowId = request("POST", "/api/workflows", create, 201).path("id").asText();
            long started = System.nanoTime();
            List<Run> runs = new ArrayList<>();
            try (var executor = Executors.newFixedThreadPool(concurrency)) {
                List<Future<Run>> futures = new ArrayList<>();
                for (int i = 0; i < executions; i++) {
                    int index = i;
                    futures.add(executor.submit(() -> execute(workflowId, scenario, index,
                            scenario.equals("retry-invalid-input") ? json.getNodeFactory().numberNode(123) : null)));
                }
                for (var future : futures) runs.add(future.get());
            }
            long duration = System.nanoTime() - started;
            ObjectNode result = json.createObjectNode().put("scenario", scenario).put("workflow_definition_id", workflowId)
                    .put("executions", executions).put("concurrency", concurrency).put("overall_elapsed", duration)
                    .put("throughput_per_second", executions * 1_000_000_000.0 / duration);
            int success = (int) runs.stream().filter(r -> r.status.equals("succeeded")).count();
            result.put("succeeded", success).put("failed", executions - success);
            result.set("observed_latency", durationSummary(runs.stream().map(r -> r.observedNanos).toList()));
            result.set("reported_latency", durationSummary(runs.stream().map(r -> r.reportedNanos).toList()));
            result.set("attempts_per_execution", integerSummary(runs.stream().map(r -> r.attempts).toList()));
            if (options.containsKey("include-runs")) {
                ArrayNode detailed = json.createArrayNode();
                for (Run run : runs) detailed.add(json.createObjectNode().put("execution_id", run.id).put("final_status", run.status)
                        .put("observed_latency", run.observedNanos).put("reported_latency", run.reportedNanos)
                        .put("attempts_observed", run.attempts).put("terminal_task_count", run.terminalTasks));
                result.set("runs", detailed);
            }
            results.add(result);
            throughput.add(result.path("throughput_per_second").asDouble());
            elapsed.add(duration);
            p95.add(result.path("reported_latency").path("p95").asLong());
            avg.add(result.path("reported_latency").path("avg").asLong());
            observedP95.add(result.path("observed_latency").path("p95").asLong());
            if (success == executions) succeededRuns++;
        }
        ObjectNode suite = json.createObjectNode().put("label", options.getOrDefault("label", ""))
                .put("captured_at", Instant.now().toString()).put("scenario", scenario).put("executions", executions)
                .put("concurrency", concurrency).put("repeat", repeat).put("poll_interval", pollInterval.toNanos())
                .put("timeout", timeout.toNanos()).put("max_attempts", attempts).put("backoff_seconds", backoff)
                .put("chain_length", chain).put("api_base_url", baseUrl).put("java_version", System.getProperty("java.version"));
        suite.set("results", results);
        ObjectNode aggregate = json.createObjectNode().put("repeat_count", repeat).put("succeeded_runs", succeededRuns)
                .put("failed_runs", repeat - succeededRuns);
        aggregate.set("throughput_per_second", floatSummary(throughput));
        aggregate.set("overall_elapsed", durationSummary(elapsed));
        aggregate.set("reported_latency_p95", durationSummary(p95));
        aggregate.set("reported_latency_avg", durationSummary(avg));
        aggregate.set("observed_latency_p95", durationSummary(observedP95));
        suite.set("aggregate", aggregate);
        String output = json.writerWithDefaultPrettyPrinter().writeValueAsString(suite) + "\n";
        if (options.containsKey("output-file")) {
            Path path = Path.of(options.get("output-file"));
            if (path.getParent() != null) Files.createDirectories(path.getParent());
            Files.writeString(path, output);
        }
        if (options.containsKey("json")) System.out.print(output);
        else System.out.printf("%s: %d runs, %d/%d successful, %.2f executions/s%n", scenario, repeat, succeededRuns, repeat,
                aggregate.path("throughput_per_second").path("avg").asDouble());
    }

    private Run execute(String workflowId, String scenario, int index, JsonNode fixedInput) throws Exception {
        long start = System.nanoTime();
        ObjectNode trigger = json.createObjectNode().put("workflow_definition_id", workflowId);
        trigger.set("input", fixedInput == null ? json.createObjectNode().put("benchmark_run", index + 1)
                .put("scenario", scenario).put("requested_at", Instant.now().toString()) : fixedInput);
        String id = request("POST", "/api/executions", trigger, 202).path("execution").path("id").asText();
        long deadline = System.nanoTime() + timeout.toNanos();
        boolean replayed = false;
        while (System.nanoTime() < deadline) {
            JsonNode snapshot = request("GET", "/api/executions/" + id, null, 200);
            JsonNode execution = snapshot.path("execution");
            String status = execution.path("status").asText();
            if (status.equals("failed") || status.equals("succeeded")) {
                if (scenario.equals("replay-missing-handler") && !replayed) {
                    String taskId = null;
                    for (JsonNode item : snapshot.path("tasks")) {
                        if (item.path("task").path("status").asText().equals("dead_lettered")) {
                            taskId = item.path("task").path("id").asText();
                            break;
                        }
                    }
                    if (taskId == null) throw new IllegalStateException("No dead-lettered task to replay");
                    request("POST", "/api/tasks/" + taskId + "/replay", null, 202);
                    replayed = true;
                    continue;
                }
                int attempts = 0;
                int terminal = 0;
                for (JsonNode item : snapshot.path("tasks")) {
                    attempts += item.path("attempts").size();
                    String taskStatus = item.path("task").path("status").asText();
                    if (taskStatus.equals("succeeded") || taskStatus.equals("dead_lettered")) terminal++;
                }
                long reported = Duration.between(Instant.parse(execution.path("started_at").asText()),
                        Instant.parse(execution.path("completed_at").asText())).toNanos();
                return new Run(id, status, System.nanoTime() - start, reported, attempts, terminal);
            }
            Thread.sleep(pollInterval);
        }
        throw new IllegalStateException("execution " + id + " did not complete before timeout");
    }

    private JsonNode request(String method, String path, JsonNode body, int expected) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(Duration.ofSeconds(10));
        if (method.equals("POST")) builder.header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : json.writeValueAsString(body)));
        else builder.GET();
        var response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != expected) throw new IllegalStateException("HTTP " + response.statusCode() + " for " + path + ": " + response.body());
        return json.readTree(response.body());
    }

    private ObjectNode scenario(String scenario, int attempts, int backoff, int chain) {
        ArrayNode tasks = json.createArrayNode();
        String entry;
        switch (scenario) {
            case "success-linear" -> {
                entry = "validate-order";
                tasks.add(task(entry, "sample.echo", "send-confirmation", attempts, backoff));
                tasks.add(task("send-confirmation", "notifications.send", null, attempts, backoff));
            }
            case "success-deep-chain" -> {
                entry = "step-1";
                for (int i = 1; i <= chain; i++) tasks.add(task("step-" + i, i % 2 == 0 ? "notifications.send" : "sample.echo",
                        i < chain ? "step-" + (i + 1) : null, attempts, backoff));
            }
            case "retry-invalid-input" -> { entry = "retry-step"; tasks.add(task(entry, "sample.echo", null, attempts, backoff)); }
            case "dead-letter-missing-handler", "replay-missing-handler" -> {
                entry = "broken-step"; tasks.add(task(entry, "missing.handler", null, attempts, backoff));
            }
            default -> throw new IllegalArgumentException("unsupported scenario " + scenario);
        }
        ObjectNode definition = json.createObjectNode().put("entry_task", entry);
        definition.set("tasks", tasks);
        return definition;
    }

    private ObjectNode task(String name, String handler, String next, int attempts, int backoff) {
        ObjectNode item = json.createObjectNode().put("name", name).put("handler_key", handler)
                .put("max_attempts", attempts).put("backoff_seconds", backoff);
        if (next != null) item.put("next_task", next);
        return item;
    }

    private ObjectNode durationSummary(List<Long> values) {
        List<Long> sorted = values.stream().sorted().toList();
        int n = sorted.size();
        return json.createObjectNode().put("count", n).put("min", sorted.getFirst())
                .put("avg", sorted.stream().mapToLong(Long::longValue).sum() / n)
                .put("p50", percentile(sorted, 0.50)).put("p95", percentile(sorted, 0.95))
                .put("p99", percentile(sorted, 0.99)).put("max", sorted.getLast());
    }

    private ObjectNode integerSummary(List<Integer> values) {
        List<Integer> sorted = values.stream().sorted().toList();
        return json.createObjectNode().put("count", sorted.size()).put("min", sorted.getFirst())
                .put("avg", sorted.stream().mapToInt(Integer::intValue).average().orElse(0))
                .put("p50", percentile(sorted, 0.50)).put("p95", percentile(sorted, 0.95))
                .put("p99", percentile(sorted, 0.99)).put("max", sorted.getLast());
    }

    private ObjectNode floatSummary(List<Double> values) {
        List<Double> sorted = values.stream().sorted().toList();
        return json.createObjectNode().put("count", sorted.size()).put("min", sorted.getFirst())
                .put("avg", sorted.stream().mapToDouble(Double::doubleValue).average().orElse(0))
                .put("p50", percentile(sorted, 0.50)).put("p95", percentile(sorted, 0.95))
                .put("p99", percentile(sorted, 0.99)).put("max", sorted.getLast());
    }

    private static <T> T percentile(List<T> sorted, double percentile) {
        return sorted.get(Math.max(0, (int) Math.ceil(percentile * sorted.size()) - 1));
    }

    private static int positive(Map<String, String> values, String key, int fallback) {
        int value = Integer.parseInt(values.getOrDefault(key, Integer.toString(fallback)));
        if (value <= 0) throw new IllegalArgumentException(key + " must be greater than 0");
        return value;
    }

    private static int nonNegative(Map<String, String> values, String key, int fallback) {
        int value = Integer.parseInt(values.getOrDefault(key, Integer.toString(fallback)));
        if (value < 0) throw new IllegalArgumentException(key + " must be at least 0");
        return value;
    }

    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> parsed = new java.util.HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String raw = args[i];
            if (!raw.startsWith("-")) throw new IllegalArgumentException("Unexpected argument: " + raw);
            String option = raw.replaceFirst("^-+", "");
            int equal = option.indexOf('=');
            if (equal >= 0) parsed.put(option.substring(0, equal), option.substring(equal + 1));
            else if (option.equals("json") || option.equals("include-runs")) parsed.put(option, "true");
            else if (i + 1 < args.length) parsed.put(option, args[++i]);
            else throw new IllegalArgumentException("Missing value for " + option);
        }
        return parsed;
    }

    private record Run(String id, String status, long observedNanos, long reportedNanos, int attempts, int terminalTasks) {}
}
