package ru.andrey.kvstorage.logic;

import ru.andrey.kvstorage.exception.DatabaseException;

import java.nio.file.Path;

@FunctionalInterface
public interface DatabaseFactory {
    Database createNonExistent(String dbName, Path dbRoot) throws DatabaseException;
}
