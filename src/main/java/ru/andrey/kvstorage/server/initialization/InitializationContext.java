package ru.andrey.kvstorage.server.initialization;

import ru.andrey.kvstorage.server.console.ExecutionEnvironment;

public interface InitializationContext {

    /**
     * Возвращает текущее окружение.
     *
     * @return текущее окружение
     */
    ExecutionEnvironment executionEnvironment();

    /**
     * Возвращает контекст инициализации для базы данных.
     *
     * @return контекст инициализации для базы данных
     */
    DatabaseInitializationContext currentDbContext();

    /**
     * Возвращает контекст инициализации для таблицы.
     *
     * @return контекст инициализации для таблицы
     */
    TableInitializationContext currentTableContext();

    /**
     * Возвращает контекст инициализации для сегмента.
     *
     * @return контекст инициализации для сегмента
     */
    SegmentInitializationContext currentSegmentContext();
}
