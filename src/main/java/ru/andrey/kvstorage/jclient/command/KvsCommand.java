package ru.andrey.kvstorage.jclient.command;

import ru.andrey.kvstorage.resp.object.RespObject;

public interface KvsCommand {

    RespObject serialize();
}
