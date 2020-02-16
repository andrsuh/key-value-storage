package ru.andrey.kvstorage.initialiation;

import ru.andrey.kvstorage.exception.DatabaseException;

public interface TableInitializer {

    void prepareContext(TableInitializationContext context) throws DatabaseException;
}
