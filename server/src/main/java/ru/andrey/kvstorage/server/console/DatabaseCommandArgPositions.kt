package ru.andrey.kvstorage.server.console

/**
 * Описывает порядок аргументов.
 * Например, используется для парсинга следубщих конструкций: "1 CREATE_DATABASE db1" или "1 SET_KEY db1 table1 key"
 */
enum class DatabaseCommandArgPositions(val positionIndex: Int) {
    COMMAND_ID(0),
    COMMAND_NAME(1),
    DATABASE_NAME(2),
    TABLE_NAME(3),
    KEY(4),
    VALUE(5);
}