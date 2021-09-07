package ru.andrey.kvstorage.server.logic.impl;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCache { // todo sukhoa add interface
    private static final int CAPACITY = 5_000;

    private final Map<String, byte[]> cache = new LinkedHashMap<>(CAPACITY, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
            return this.size() >= CAPACITY;
        }
    };

    public byte[] get(String key) {
        return cache.get(key);
    }

    public void set(String key, byte[] value) {
        cache.put(key, value);
    }

    public void delete(String key) {
        cache.remove(key);
    }
}
