package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.server.exception.DatabaseException;

public interface DatabaseCommand {

    /**
     * Запуск данной команды.
     *
     * @return результат выполнение команды
     * @throws DatabaseException если произошла ошибка СУБД
     */
    DatabaseCommandResult execute() throws DatabaseException;
}
