package ru.andrey.kvstorage.initialiation;

import ru.andrey.kvstorage.exception.DatabaseException;

public interface DatabaseInitializer {

    void prepareContext(DatabaseInitializationContext context) throws DatabaseException;
}
