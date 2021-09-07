package ru.andrey.kvstorage.jclient.exception;

/**
 * Ошибка подключения
 */
public class ConnectionException extends Exception {

    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectionException(String message) {
        super(message);
    }
}
