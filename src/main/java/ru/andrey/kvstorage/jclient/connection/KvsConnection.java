package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.resp.object.RespObject;

public interface KvsConnection extends AutoCloseable {

    RespObject send(int commandId, RespObject command);

    @Override
    void close();
}
