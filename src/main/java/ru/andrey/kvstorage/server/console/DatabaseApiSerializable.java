package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.resp.object.RespObject;

public interface DatabaseApiSerializable {
    RespObject serialize();
}
