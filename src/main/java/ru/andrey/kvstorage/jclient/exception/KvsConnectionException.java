package ru.andrey.kvstorage.jclient.exception;

public class KvsConnectionException extends Exception {
    public KvsConnectionException(String message) {
        super(message);
    }

    public KvsConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
