package io.durableflow.persistence;

public final class StoreExceptions {
    private StoreExceptions() {}
    public static final class NotFound extends RuntimeException {
        public NotFound() { super("no rows in result set"); }
    }
    public static final class NotReplayable extends RuntimeException {
        public NotReplayable() { super("task is not dead-lettered"); }
    }
    public static final class IdempotencyInProgress extends RuntimeException {
        public IdempotencyInProgress() { super("idempotency key is already in progress"); }
    }
}
