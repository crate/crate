package io.crate.executor;

public class ExecutionException extends RuntimeException {
    public ExecutionException(String message) {
        super(message);
    }
}
