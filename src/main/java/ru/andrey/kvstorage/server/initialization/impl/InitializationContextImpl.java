package ru.andrey.kvstorage.server.initialization.impl;

import lombok.Builder;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.initialization.DatabaseInitializationContext;
import ru.andrey.kvstorage.server.initialization.InitializationContext;
import ru.andrey.kvstorage.server.initialization.SegmentInitializationContext;
import ru.andrey.kvstorage.server.initialization.TableInitializationContext;

@Builder
public class InitializationContextImpl implements InitializationContext {

    private final ExecutionEnvironment executionEnvironment;
    private final DatabaseInitializationContext currentDatabaseContext;
    private final TableInitializationContext currentTableContext;
    private final SegmentInitializationContext currentSegmentContext;

    private InitializationContextImpl(ExecutionEnvironment executionEnvironment,
                                      DatabaseInitializationContext currentDatabaseContext,
                                      TableInitializationContext currentTableContext,
                                      SegmentInitializationContext currentSegmentContext) {
        this.executionEnvironment = executionEnvironment;
        this.currentDatabaseContext = currentDatabaseContext;
        this.currentTableContext = currentTableContext;
        this.currentSegmentContext = currentSegmentContext;
    }

    @Override
    public ExecutionEnvironment executionEnvironment() {
        return executionEnvironment;
    }

    @Override
    public DatabaseInitializationContext currentDbContext() {
        return currentDatabaseContext;
    }

    @Override
    public TableInitializationContext currentTableContext() {
        return currentTableContext;
    }

    @Override
    public SegmentInitializationContext currentSegmentContext() {
        return currentSegmentContext;
    }
}
