package ru.andrey.kvstorage.server.initialization.impl;

import lombok.Builder;
import ru.andrey.kvstorage.server.index.impl.SegmentIndex;
import ru.andrey.kvstorage.server.initialization.SegmentInitializationContext;

import java.nio.file.Path;

@Builder
public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final String segmentName;
    private final Path segmentPath;
    private final int currentSize;
    private final SegmentIndex index; // todo sukhoa think of better design

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, int currentSize) {
        this(segmentName, tablePath.resolve(segmentName), currentSize, null); // todo sukhoa maybe not null?
    }

    @Override
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public Path getSegmentPath() {
        return segmentPath;
    }

    @Override
    public long getCurrentSize() {
        return currentSize;
    }

    @Override
    public SegmentIndex getIndex() {
        return index;
    }
}
