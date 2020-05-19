package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.jclient.exception.KvsConnectionException;
import ru.andrey.kvstorage.resp.object.RespObject;

public interface KvsConnection extends AutoCloseable {

    RespObject send(RespObject command);

    @Override
    void close() throws KvsConnectionException;
}
