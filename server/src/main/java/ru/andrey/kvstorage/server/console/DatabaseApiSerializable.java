package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.resp.object.RespObject;

public interface DatabaseApiSerializable {
    /**
     * Возвращает представление данного объекта в RESP протоколе.
     *
     * @return представление данного объекта в RESP протоколе
     */
    RespObject serialize();
}
