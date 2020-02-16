package ru.andrey.kvstorage.initialiation;

import ru.andrey.kvstorage.logic.Segment;

import java.nio.file.Path;
import java.util.Map;

public interface TableInitializationContext {
    String getTableName();

    void setTableName(String tableName);

    Path getTablePath();

    void setTablePath(Path tablePath);

    Segment getCurrentSegment();

    void setCurrentSegment(Segment currentSegment);

    Map<String, Segment> getSegments();

    void setSegments(Map<String, Segment> segments);
}
