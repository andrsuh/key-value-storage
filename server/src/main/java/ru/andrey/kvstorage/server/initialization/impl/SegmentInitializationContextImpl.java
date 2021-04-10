package ru.andrey.kvstorage.server.initialization.impl;

import lombok.Builder;
import lombok.Getter;
import ru.andrey.kvstorage.server.index.impl.SegmentIndex;
import ru.andrey.kvstorage.server.initialization.SegmentInitializationContext;

import java.nio.file.Path;

@Getter
@Builder
public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final String segmentName;
    private final Path segmentPath;
    private final long currentSize;
    private final SegmentIndex index; // todo sukhoa think of better design

    private SegmentInitializationContextImpl(String segmentName, Path segmentPath, long currentSize, SegmentIndex index) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, long currentSize) {
        this(segmentName, tablePath.resolve(segmentName), currentSize, new SegmentIndex());
    }
}
