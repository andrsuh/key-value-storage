package ru.andrey.kvstorage.jclient.client;

public interface KvsClient {
    String get(String tableName, String key);

    String set(String tableName, String key, String value);
}
