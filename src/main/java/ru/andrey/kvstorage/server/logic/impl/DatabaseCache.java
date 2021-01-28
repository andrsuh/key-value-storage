package ru.andrey.kvstorage.server.logic.impl;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCache { // todo sukhoa add interface
    private static final int CAPACITY = 5_000;

    private final Map<String, String> cache = new LinkedHashMap<>(CAPACITY, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return this.size() >= CAPACITY;
        }
    };

    public String get(String key) {
        return cache.get(key);
    }

    public void set(String key, String value) {
        cache.put(key, value);
    }

    public void delete(String key) {
        cache.remove(key);
    }
}