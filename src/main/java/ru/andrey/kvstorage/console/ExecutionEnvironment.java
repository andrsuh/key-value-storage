package ru.andrey.kvstorage.console;

import ru.andrey.kvstorage.logic.Database;

import java.util.Optional;

public interface ExecutionEnvironment {
    Optional<Database> currentDatabase();

    void setCurrentDatabase(Database db);// TODO A: name as an input param and get db from map instead

    Optional<Database> getDatabase(String name);

    void addDatabase(Database db);
}
