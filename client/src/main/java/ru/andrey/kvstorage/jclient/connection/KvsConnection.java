package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.resp.object.RespObject;

/**
 * Определяет интерфейс подключения к key value storage
 */
public interface KvsConnection extends AutoCloseable {
    /**
     * Отправляет команду к серверу
     *
     * @param commandId id команды (номер)
     * @param command   команда
     * @return Результат исполнения
     */
    RespObject send(int commandId, RespObject command);

    @Override
    void close();
}
