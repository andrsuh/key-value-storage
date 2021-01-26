package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.DatabaseServer;

public class DirectReferenceKvsConnection implements KvsConnection {

    private final DatabaseServer databaseServer;

    public DirectReferenceKvsConnection(DatabaseServer databaseServer) {
        this.databaseServer = databaseServer;
    }

    @Override
    public RespObject send(RespObject object) {
        return databaseServer.executeNextCommand(object.asString()).serialize();
    }

    @Override
    public void close() {}
}
