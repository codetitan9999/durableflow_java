# Java Benchmarks

The [Java benchmark driver](../src/main/java/io/durableflow/bench/BenchmarkMain.java) creates workflows through the API, triggers executions concurrently, and polls snapshots until they reach terminal state. It reports throughput and latency from the real PostgreSQL/Redis worker path, not an in-process shortcut.

## Run

Start the Java stack with `docker compose up --build`, then run:

```bash
mvn -q compile exec:java -Dexec.mainClass=io.durableflow.bench.BenchmarkMain -Dexec.args='--scenario success-linear --executions 25 --concurrency 5 --json --output-file benchmarks/java-local.json'
```

The [suite script](../scripts/run_bench_suite.sh) exercises baseline and tuned publisher cadence, multiple workers, worker interruption, retry, dead-letter, and replay. It writes results under `benchmarks/java-results/`. The [chart script](../scripts/generate_benchmark_charts.sh) generates summaries from JSON output.

Supported scenarios: `success-linear`, `success-deep-chain`, `retry-invalid-input`, `dead-letter-missing-handler`, and `replay-missing-handler`.

## Current Evidence

All five scenarios completed small smoke runs against the Java API. These establish that the harness works; they are **not** capacity or latency claims. A representative Java load suite, repeated runs, resource observations, and failure-boundary measurements are still needed before citing performance numbers.

Publish performance numbers only with the workload, hardware, Compose topology, polling settings, and raw JSON artifacts used to produce them.
