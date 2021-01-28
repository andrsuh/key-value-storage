package ru.andrey.kvstorage.server.logic;

import ru.andrey.kvstorage.server.exception.DatabaseException;

import java.util.Optional;

public interface Database {
    String getName();

    void createTableIfNotExists(String tableName) throws DatabaseException;

    void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException;

    void write(String tableName, String objectKey, String objectValue) throws DatabaseException;

    Optional<String> read(String tableName, String objectKey) throws DatabaseException;

    void delete(String tableName, String objectKey) throws DatabaseException;
}