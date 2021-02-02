package ru.andrey.kvstorage.server.initialization;

import ru.andrey.kvstorage.server.console.ExecutionEnvironment;

public interface InitializationContext {
    ExecutionEnvironment executionEnvironment();

    DatabaseInitializationContext currentDbContext();

    TableInitializationContext currentTableContext();

    SegmentInitializationContext currentSegmentContext();
}
