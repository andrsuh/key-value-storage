package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.server.exception.DatabaseException;

public interface DatabaseCommand {
    DatabaseCommandResult execute() throws DatabaseException;
}
