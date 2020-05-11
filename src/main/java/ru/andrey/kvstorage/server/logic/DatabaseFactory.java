package ru.andrey.kvstorage.server.logic;

import ru.andrey.kvstorage.server.exception.DatabaseException;

import java.nio.file.Path;

@FunctionalInterface
public interface DatabaseFactory {
    Database createNonExistent(String dbName, Path dbRoot) throws DatabaseException;
}
