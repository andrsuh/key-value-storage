package ru.andrey.kvstorage.initialization;

import ru.andrey.kvstorage.index.TableIndex;
import ru.andrey.kvstorage.logic.Segment;

import java.nio.file.Path;

public interface TableInitializationContext {
    String getTableName();

    Path getTablePath();

    TableIndex getTableIndex();

    Segment getCurrentSegment();

    void updateCurrentSegment(Segment segment); // todo sukhoa refactor?
}
