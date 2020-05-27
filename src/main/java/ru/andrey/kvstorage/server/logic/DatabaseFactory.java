package ru.andrey.kvstorage.server.logic;

import ru.andrey.kvstorage.server.exception.DatabaseException;

import java.nio.file.Path;

@FunctionalInterface
public interface DatabaseFactory {

    /**
     * Создает базу данных с указанным именем, если это имя еще не занято.
     *
     * @param dbName имя базы данных
     * @param dbRoot путь до директории, в которой будет создана база данных
     * @return объект созданной таблицы
     * @throws DatabaseException если база данных с данным именем уже существует или если произошла ошибка ввода-вывода
     */
    Database createNonExistent(String dbName, Path dbRoot) throws DatabaseException;
}
