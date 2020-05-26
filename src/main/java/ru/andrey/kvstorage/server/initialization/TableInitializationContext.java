package ru.andrey.kvstorage.server.initialization;

import ru.andrey.kvstorage.server.index.impl.TableIndex;
import ru.andrey.kvstorage.server.logic.Segment;

import java.nio.file.Path;

public interface TableInitializationContext {
    String getTableName();

    Path getTablePath();

    TableIndex getTableIndex();

    Segment getCurrentSegment();

    void updateCurrentSegment(Segment segment); // todo sukhoa refactor?
}
