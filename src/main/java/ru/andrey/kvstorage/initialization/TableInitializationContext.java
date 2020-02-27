package ru.andrey.kvstorage.initialization;

import ru.andrey.kvstorage.index.Index;
import ru.andrey.kvstorage.logic.Segment;

import java.nio.file.Path;

public interface TableInitializationContext {
    String getTableName();

    Path getTablePath();

    Index<String, Segment> getTableIndex();

    Segment getCurrentSegment();

    void updateCurrentSegment(Segment segment); // todo sukhoa refactor?
}
