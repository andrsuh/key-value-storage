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
        // TODO A: name as an input param and get db from map instead
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
