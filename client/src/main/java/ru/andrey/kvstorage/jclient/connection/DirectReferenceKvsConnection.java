package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.connector.JavaSocketServerConnector;

public class DirectReferenceKvsConnection implements KvsConnection {

    private final JavaSocketServerConnector databaseServer;

    public DirectReferenceKvsConnection(JavaSocketServerConnector databaseServer) {
        this.databaseServer = databaseServer;
    }

    @Override
    public RespObject send(int commandId, RespObject command) {
        return databaseServer.executeNextCommand((RespArray) command).serialize();
    }

    @Override
    public void close() {}
}
