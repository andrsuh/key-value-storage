package ru.andrey.kvstorage.jclient.command;

import ru.andrey.kvstorage.resp.object.RespObject;

import java.util.concurrent.atomic.AtomicInteger;

public interface KvsCommand {
    AtomicInteger idGen = new AtomicInteger();

    RespObject serialize();

    int getCommandId();
}
