package ru.andrey.kvstorage.server.initialization;

import ru.andrey.kvstorage.server.exception.DatabaseException;

public interface Initializer {
    /**
     * Выполняет инициализацию какой-либо сущности с переданным контекстом.
     *
     * @param context контекст, с которым нужно провести инициализацию
     * @throws DatabaseException если произошла ошибка инициализации сущности
     */
    void perform(InitializationContext context) throws DatabaseException;
}
