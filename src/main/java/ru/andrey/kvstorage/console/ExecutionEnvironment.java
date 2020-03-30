package ru.andrey.kvstorage.console;

import ru.andrey.kvstorage.logic.Database;

import java.util.Optional;

public interface ExecutionEnvironment {
    Optional<Database> getDatabase(String name);

    void addDatabase(Database db);
}
