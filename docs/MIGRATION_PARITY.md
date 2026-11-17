# Java migration parity checklist

Status: Java API, worker, PostgreSQL store, Redis Streams transport, and Docker Compose stack run end to end. Differential smoke tests passed for key API and failure paths before the repositories were separated. Full benchmark and crash-interruption validation remain before claiming complete parity or production readiness.

Baseline: [original Go repository](https://github.com/codetitan9999/DurableWorkFlowEngine), commit 9ae7ba7. This repository contains only the Java application, shared UI, schema, and operational assets; Go benchmark artifacts are not Java measurements.

## Completion gates

- [ ] Audit every Go module, test, script, configuration file, and external contract.
- [x] Implement Java 21 / Spring Boot API and worker with an independent Maven build.
- [x] Preserve database schema and migration history compatibility.
- [ ] Port unit and integration tests, with a mapping to source tests.
- [x] Run API differential smoke tests against isolated Go and Java environments before splitting repositories.
- [x] Validate crash recovery, retry scheduling, replay, and transaction rollback in focused integration tests.
- [ ] Validate dashboard, Postman, metrics, tracing, alerts, and Grafana integration; dashboard service, metrics, traces, and Grafana are checked, but visual/alert/Postman runs remain.
- [ ] Port benchmark runner and retain CLI/report compatibility; Java runner works, but representative Java suite and chart validation remain.
- [x] Update Docker, CI, startup instructions, architecture, and diagrams.
- [ ] Confirm no unresolved behavior differences before claiming parity.

## Feature inventory

| Area | Required parity | Status |
| --- | --- | --- |
| Definitions | Default definition, name trimming, uniqueness, validation, entry and next-task resolution, retry defaults | Parser, service and store implemented; differential checks pending |
| HTTP | All routes and aliases, request decoding, JSON fields and omissions, errors, status codes, CORS, OPTIONS, health and metrics | Core routes, health and metrics implemented; malformed JSON, limits, aliases, missing IDs and happy path compared with Go |
| Execution | Execution + initial task + outbox in one transaction; input normalization and stable task keys | Implemented and covered by PostgreSQL integration test; edge cases pending |
| Snapshots | Execution, tasks ordered by creation, attempts grouped and ordered; empty collections | Implemented; integration test covers empty attempts, other cases pending |
| Task attempts | Row locking, attempt numbering, terminal-state skip, running-task redelivery | Store and worker implemented; combined PostgreSQL/Redis crash-reclaim integration test passes |
| Chaining | Atomic completion plus next task/outbox; previous output becomes next input; rollback on conflicts | Store and worker implemented; rollback integration test passes |
| Retries | Fixed delay, max-attempt semantics, persisted next_run_at, SKIP LOCKED enqueue, batch size 20 | Implemented; due-enqueue and worker-level retry integration tests pass |
| Dead letters | Immediate missing-handler failure, exhausted attempts, durable error and timestamps, list limits | Implemented; missing-handler flow smoke-tested in Compose |
| Replay | Locked eligibility check; same task/key/input; preserve attempts; reset eligible state and create outbox atomically | Implemented and covered by PostgreSQL integration test; differential checks pending |
| Idempotency | Composite key, ownership, cached response, same-owner continuation, conflict, guarded release | Implemented; PostgreSQL integration test passes |
| Handlers | sample.echo and notifications.send response fields, timestamps, invalid input, release on errors | Implemented; happy-path Compose smoke and invalid-input retry test pass; exact error-text comparison pending |
| Outbox | Poll cadence, due retries, due event ordering, batch limit, failure accounting, duplicate publication window | Publisher implemented; Go-like duplicate publication window retained; retry failure path pending |
| Redis | Stream encoding, group creation, sequential consumption, reclaim before fresh reads, options, ACK and error semantics | Implemented; reclaim, ACK, and poison-message Testcontainers tests pass |
| Persistence | SQL schemas, advisory migration lock, migration filenames/history, transactions, connection pool of eight | Implemented; Compose migration smoke passed; race validation pending |
| Configuration | All original env variables, defaults, duration parsing, validation, API/worker identities and addresses | Main values, required connections, startup timeout, and Go-style duration parsing implemented; validation matrix pending |
| Lifecycle | Startup readiness retry, timeouts, cancellation, shutdown, consumer failure propagation | Startup retry and fatal background shutdown implemented; signal/shutdown tests pending |
| Telemetry | Existing Prometheus names/labels/buckets, HTTP and task measurements, OTLP tracing, service naming | Both Prometheus targets up and collector received spans in full Compose test; bucket/alert parity pending |
| Dashboard | Existing React/TypeScript UI and API compatibility | Build passes, Compose serves UI, and Java API happy path works; visual validation pending |
| Operations | Docker services, benchmark workers, collector, Prometheus alerts, Grafana provisioning, Postman | Full Compose stack started; Grafana dashboard provisioned; alerts/Postman run pending |
| Benchmarks | All scenarios, concurrency, repetitions, timeouts, polling, percentiles, aggregate report and charts | Java CLI and suite script implemented; all five scenarios smoke-tested; full representative suite and fresh charts pending |
| Documentation | Java build/run/test instructions, source links, class/ER/sequence diagrams, known limits | Java README, architecture, and LLD diagrams added |

## Verification performed

- `mvn verify`: Java unit, HTTP contract, PostgreSQL transaction/recovery, Redis reclaim/poison-message, and combined worker-crash recovery tests pass.
- `npm --prefix apps/web run build`: dashboard builds. `npm audit --omit=dev` reports no production-package advisories; development dependencies still have advisories.
- Differential smoke checks were run before this Java-only repository was separated. They compared stable HTTP fields for create/trigger/snapshot, invalid requests, retries, dead-letter listing, and replay, plus metrics endpoints.
- Full Java Compose stack: `/healthz`, dashboard HTML, Prometheus `up=1` for API and worker, provisioned Grafana dashboard, and OTLP traces in collector debug logs were observed. Test containers and volumes were removed afterward.
- Java benchmark CLI smoke: all five scenarios ran against the Java API. These tiny runs are not capacity measurements.

## Behavior requiring explicit verification

The source permits overlapping attempts for the same task and has no lease or fencing. Handler reservations use task ownership rather than attempt ownership. An old duplicate can bypass a scheduled retry delay. Replay retains the total attempt count. Outbox publication does not exclusively claim rows. Snapshot reads are separate queries. Invalid stream payloads exit the consumer. These behaviors must be documented and covered; changes require an explicit compatibility decision rather than silent fixes during migration.

Production readiness is a separate assessment from behavioral parity. This checklist does not certify either until validation evidence is recorded.
