package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.server.logic.Database;

import java.util.Optional;

public interface ExecutionEnvironment {
    Optional<Database> getDatabase(String name);

    void addDatabase(Database db);
}
