package ru.andrey.kvstorage.jclient.client;

public interface KvsClient {
    String read(String tableName, String key);

    String upsert(String tableName, String key, String value);
}
