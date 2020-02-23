package ru.andrey.kvstorage.console.impl;

import ru.andrey.kvstorage.console.ExecutionEnvironment;
import ru.andrey.kvstorage.logic.Database;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private final Map<String, Database> dbs = new HashMap<>();
    private Database current;

    @Override
    public Optional<Database> currentDatabase() {
        return Optional.ofNullable(current);
    }

    @Override
    public void setCurrentDatabase(Database db) {
        this.current = db;
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        return Optional.ofNullable(dbs.get(name));
    }

    @Override
    public void addDatabase(Database db) {
        dbs.put(db.getName(), db);
    }
}
