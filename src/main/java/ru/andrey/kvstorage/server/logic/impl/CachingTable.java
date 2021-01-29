package ru.andrey.kvstorage.server.logic.impl;

import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.Table;

import java.util.Optional;

/**
 * Декторато для таблицы. Кэширует данные
 */
public class CachingTable implements Table {
    private final Table table;
    private final DatabaseCache cache;

    public CachingTable(Table table) {
        this(table, new DatabaseCache());
    }

    public CachingTable(Table table, DatabaseCache cache) {
        this.table = table;
        this.cache = cache;
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        table.write(objectKey, objectValue);
        cache.set(objectKey, objectValue);
    }

    @Override
    public Optional<String> read(String objectKey) throws DatabaseException {
        String value = cache.get(objectKey);
        if (value == null) {
            Optional<String> result = table.read(objectKey);
            result.ifPresent(s -> cache.set(objectKey, s));
            return result;
        }
        return Optional.of(value);
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        table.delete(objectKey);
        cache.delete(objectKey);
    }
}
