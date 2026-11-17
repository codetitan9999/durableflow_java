# Verification and Limits

DurableFlow's Java API, publisher, worker, dashboard, PostgreSQL, and Redis Streams run together in Docker Compose. The checks below describe what has been exercised; they are not a production-readiness certification.

## Checks Performed

- `mvn verify` passes unit and Testcontainers integration tests for definition validation, HTTP responses, transactional writes and rollback, retry scheduling, dead-letter replay, idempotency conflicts, Redis reclaim, and worker recovery.
- `npm --prefix apps/web run build` builds the React dashboard.
- GitHub Actions runs backend tests, the web build, and shell-script validation on pushes and pull requests.
- A full Compose stack served the dashboard and both health endpoints. Prometheus scraped the API and worker; Grafana loaded the provisioned dashboard; the collector received OTLP spans.
- All five benchmark scenarios completed small Java API smoke runs. They verify the harness, not a throughput or latency limit.

## Current Boundaries

- Workflows are linear; there is no branching or parallel-step scheduler.
- A running task has no independent lease, heartbeat, or fencing token. Recovery can overlap attempts, so external side effects still require their own idempotency guarantees.
- Outbox rows are not exclusively claimed across multiple publishers. The single-publisher Compose setup is exercised; multi-publisher contention is not.
- Replay retains the task's ID, input, idempotency key, and accumulated attempt count.
- The included handlers produce demo JSON responses; neither calls an external service.

## Not Yet Measured

Representative Java load, sustained throughput, resource use, and tail latency under worker interruption have not been established. See [benchmarks](benchmarks.md) for the available harness and [operations](operations.md) for live health and telemetry checks.
