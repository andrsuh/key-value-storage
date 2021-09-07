package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.DatabaseServer;

/**
 * Реализация коннекшена, когда есть прямая ссылка на объект
 */
public class DirectReferenceKvsConnection implements KvsConnection {

    private final DatabaseServer databaseServer;

    public DirectReferenceKvsConnection(DatabaseServer databaseServer) {
        this.databaseServer = databaseServer;
    }

    @Override
    public RespObject send(int commandId, RespArray command) {
        return databaseServer.executeNextCommand(command).join().serialize();
    }

    @Override
    public void close() {}
}
