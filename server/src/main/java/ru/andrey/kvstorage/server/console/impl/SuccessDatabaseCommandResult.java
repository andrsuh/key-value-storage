package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;

/**
 * Результат успешной команды
 */
public class SuccessDatabaseCommandResult implements DatabaseCommandResult {

    private final byte[] payload;

    public SuccessDatabaseCommandResult(byte[] payload) {
        this.payload = payload;
    }

    @Override
    public String getPayLoad() {
        if (payload == null)
            return null;
        return new String(payload);
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    /**
     * Сериализуется в {@link RespBulkString}
     */
    @Override
    public RespObject serialize() {
        if (payload != null) {
            return new RespBulkString(payload);
        }
        return new RespBulkString(null);
    }
}
