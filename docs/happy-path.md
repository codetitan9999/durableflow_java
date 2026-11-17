# One Execution, End to End

1. [WorkflowController](../src/main/java/io/durableflow/http/WorkflowController.java) accepts a definition and later an execution request.
2. [WorkflowService](../src/main/java/io/durableflow/orchestrator/WorkflowService.java) validates the definition and asks [JdbcWorkflowStore](../src/main/java/io/durableflow/persistence/JdbcWorkflowStore.java) to commit the execution, first task, and outbox event together.
3. [OutboxPublisher](../src/main/java/io/durableflow/outbox/OutboxPublisher.java) reads due events and writes a task message to [RedisStreams](../src/main/java/io/durableflow/queue/RedisStreams.java), then records dispatch. A crash between those writes can duplicate delivery.
4. [TaskWorker](../src/main/java/io/durableflow/orchestrator/TaskWorker.java) reloads durable task state, records an attempt, and selects a [TaskHandler](../src/main/java/io/durableflow/handlers/TaskHandler.java) by key.
5. The handler uses [JdbcIdempotencyStore](../src/main/java/io/durableflow/persistence/JdbcIdempotencyStore.java) to reserve or reuse a stored response. On success, the store completes the task and either creates the next task plus outbox event or completes the execution.
6. The worker acknowledges the Redis message after the durable outcome. The dashboard reads the execution snapshot from PostgreSQL through the API.

For failure paths and transaction boundaries, see [low-level design](low-level-design.md).
