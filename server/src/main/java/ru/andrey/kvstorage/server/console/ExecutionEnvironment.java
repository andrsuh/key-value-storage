package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.server.logic.Database;

import java.nio.file.Path;
import java.util.Optional;

public interface ExecutionEnvironment {
    /**
     * Возвращает путь до папки, где находятся базы данных.
     *
     * @return путь до папки, где находятся базы данных
     */
    Path getWorkingPath();

    /**
     * Возвращает {@code Optional<Database>}.
     *
     * @param name имя базы данных
     * @return {@code Optional<Database>}
     */
    Optional<Database> getDatabase(String name);

    /**
     * Добавляет базу данных в текущее окружение.
     *
     * @param db база данных, которую нужно добавить
     */
    void addDatabase(Database db);
}