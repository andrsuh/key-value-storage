package ru.andrey.kvstorage.server.console

enum class DatabaseCommandArgPositions(val positionIndex: Int) {
    COMMAND_ID(0),
    COMMAND_NAME(1),
    DATABASE_NAME(2),
    TABLE_NAME(3),
    KEY(4),
    VALUE(5);
}