package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.DatabaseServer;
import ru.andrey.kvstorage.jclient.exception.KvsConnectionException;

public class DirectReferenceConnection implements KvsConnection {
    private final DatabaseServer databaseServer;

    public DirectReferenceConnection(DatabaseServer databaseServer) {
        this.databaseServer = databaseServer;
    }

    @Override
    public void connect() throws KvsConnectionException {

    }

    @Override
    public void close() throws KvsConnectionException {

    }

    @Override
    public byte[] send(byte[] command) {
        return databaseServer.executeNextCommandAndGetApiBytes(command);
    }
}
