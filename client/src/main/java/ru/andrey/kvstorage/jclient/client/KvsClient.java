package ru.andrey.kvstorage.jclient.client;

import ru.andrey.kvstorage.jclient.exception.DatabaseExecutionException;

/**
 * Клиент для доступа к БД
 */
public interface KvsClient {
    String createDatabase() throws DatabaseExecutionException;

    String createTable(String tableName) throws DatabaseExecutionException;

    String get(String tableName, String key) throws DatabaseExecutionException;

    String set(String tableName, String key, String value) throws DatabaseExecutionException;

    String delete(String tableName, String key) throws DatabaseExecutionException;
}
