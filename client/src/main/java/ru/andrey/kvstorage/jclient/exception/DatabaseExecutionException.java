package ru.andrey.kvstorage.jclient.exception;

public class DatabaseExecutionException extends Exception {
    public DatabaseExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseExecutionException(String message) {
        super(message);
    }
}
