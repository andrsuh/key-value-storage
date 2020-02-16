package ru.andrey.kvstorage.initialiation.impl;

import ru.andrey.kvstorage.initialiation.SegmentInitializationContext;
import ru.andrey.kvstorage.logic.Index;
import ru.andrey.kvstorage.logic.SegmentIndex;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private String segmentName;
    private Path segmentPath;
    private int currentSize;
    private Index segmentIndex = new SegmentIndex(); // todo sukhoa think of better design

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, int currentSize) {
        this.segmentName = segmentName;
        this.segmentPath = tablePath.resolve(segmentName);
        this.currentSize = currentSize;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }

    public Path getSegmentPath() {
        return segmentPath;
    }

    public void setSegmentPath(Path segmentPath) {
        this.segmentPath = segmentPath;
    }

    public int getCurrentSize() {
        return currentSize;
    }

    public void setCurrentSize(int currentSize) {
        this.currentSize = currentSize;
    }

    public Index getSegmentIndex() {
        return segmentIndex;
    }

    public void setSegmentIndex(Index segmentIndex) {
        this.segmentIndex = segmentIndex;
    }
}
