package ru.andrey.kvstorage.initialization.impl;

import lombok.Getter;
import ru.andrey.kvstorage.index.Index;
import ru.andrey.kvstorage.initialization.TableInitializationContext;
import ru.andrey.kvstorage.logic.Segment;

import java.nio.file.Path;

@Getter
public class TableInitializationContextImpl implements TableInitializationContext {
    private final String tableName;
    private final Path tablePath;
    private final Index<String, Segment> tableIndex;
    private volatile Segment currentSegment;

    public TableInitializationContextImpl(String tableName, Path databasePath, Index<String, Segment> tableIndex) {
        this.tableName = tableName;
        this.tablePath = databasePath.resolve(tableName);
        this.tableIndex = tableIndex;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        this.currentSegment = segment;
    }
}
