package ru.andrey.kvstorage.initialization;

import ru.andrey.kvstorage.exception.DatabaseException;

public interface Initializer {
    void perform(InitializationContext context) throws DatabaseException;
}
