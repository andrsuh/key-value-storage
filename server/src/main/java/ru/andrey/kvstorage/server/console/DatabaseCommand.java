package ru.andrey.kvstorage.server.console;

public interface DatabaseCommand {
    /**
     * Запускает команду.
     *
     * @return Сообщение о выполнении результата команды.
     */
    DatabaseCommandResult execute();
}
