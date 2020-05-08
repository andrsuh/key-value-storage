package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.jclient.exception.KvsConnectionException;

public interface KvsConnection extends AutoCloseable {
    void connect() throws KvsConnectionException;

    byte[] send(byte[] command);

    @Override
    void close() throws KvsConnectionException;
}
