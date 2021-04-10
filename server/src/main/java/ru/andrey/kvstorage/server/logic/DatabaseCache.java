package ru.andrey.kvstorage.server.logic;

public interface DatabaseCache {
    byte[] get(String key);

    void set(String key, byte[] value);

    void delete(String key);
}
