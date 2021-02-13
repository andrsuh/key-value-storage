package ru.andrey.kvstorage.server.initialization;

import ru.andrey.kvstorage.server.exception.DatabaseException;

public interface Initializer {
    void perform(InitializationContext context) throws DatabaseException;
}
