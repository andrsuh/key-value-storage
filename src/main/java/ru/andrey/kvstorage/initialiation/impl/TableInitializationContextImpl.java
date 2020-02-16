package ru.andrey.kvstorage.initialiation.impl;

import ru.andrey.kvstorage.initialiation.TableInitializationContext;
import ru.andrey.kvstorage.logic.Segment;

import java.nio.file.Path;
import java.util.Map;

public class TableInitializationContextImpl implements TableInitializationContext {
    private String tableName;
    private Path tablePath;
    private Segment currentSegment;
    private Map<String, Segment> segments;

    public TableInitializationContextImpl(String tableName, Path databasePath) {
        this.tableName = tableName;
        this.tablePath = databasePath.resolve(tableName);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Path getTablePath() {
        return tablePath;
    }

    public void setTablePath(Path tablePath) {
        this.tablePath = tablePath;
    }

    public Segment getCurrentSegment() {
        return currentSegment;
    }

    public void setCurrentSegment(Segment currentSegment) {
        this.currentSegment = currentSegment;
    }

    public Map<String, Segment> getSegments() {
        return segments;
    }

    public void setSegments(Map<String, Segment> segments) {
        this.segments = segments;
    }
}
