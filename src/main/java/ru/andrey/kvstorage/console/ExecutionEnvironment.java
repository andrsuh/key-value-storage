package ru.andrey.kvstorage.console;

import ru.andrey.kvstorage.logic.Database;

import java.util.Optional;

public interface ExecutionEnvironment {
    Optional<Database> currentDatabase();

    void setCurrentDatabase(Database db);

    Optional<Database> getDatabase(String name);

    void addDatabase(Database db);
}
