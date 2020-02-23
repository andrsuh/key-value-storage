package ru.andrey.kvstorage.initialization;

import ru.andrey.kvstorage.console.ExecutionEnvironment;

public interface InitializationContext {
    ExecutionEnvironment executionEnvironment();

    DatabaseInitializationContext currentDbContext();

    TableInitializationContext currentTableContext();

    SegmentInitializationContext currentSegmentContext();
}
