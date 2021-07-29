package ru.andrey.kvstorage.jclient.command;

import ru.andrey.kvstorage.resp.object.RespArray;

import java.util.concurrent.atomic.AtomicInteger;

public interface KvsCommand {
    /**
     * Счетчик для команды. Каждая созданная команда использует это поле для создания id, инкрементирует значение
     * Первая комада создается с id 0
     */
    AtomicInteger idGen = new AtomicInteger();

    /**
     * Сериализует объект в RESP
     *
     * @return RESP объект
     */
    RespArray serialize();

    /**
     * Id команды
     *
     * @return id
     */
    int getCommandId();
}
