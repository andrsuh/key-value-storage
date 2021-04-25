package ru.andrey.kvstorage.jclient.client;

/**
 * Клиент для доступа к БД
 */
public interface KvsClient {
    String createDatabase();

    String createTable(String tableName);

    String get(String tableName, String key);

    String set(String tableName, String key, String value);

    String delete(String tableName, String key);
}
