package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.server.exception.DatabaseException;

public interface DatabaseCommand {
    /**
     * Запускает команду.
     *
     * @return результат выполнение команды
     * @throws DatabaseException если произошла ошибка СУБД
     */
    DatabaseCommandResult execute() throws DatabaseException;
}
