package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.jclient.exception.ConnectionException;
import ru.andrey.kvstorage.resp.object.RespArray;
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
     * @throws ConnectionException если не удалось прочитать ответ
     */
    RespObject send(int commandId, RespArray command) throws ConnectionException;
}
