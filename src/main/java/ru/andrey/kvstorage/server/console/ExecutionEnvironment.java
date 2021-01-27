package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.server.logic.Database;

import java.nio.file.Path;
import java.util.Optional;

public interface ExecutionEnvironment {
    Path getWorkingPath();

    int getPort();

    Optional<Database> getDatabase(String name);

    void addDatabase(Database db);
}
