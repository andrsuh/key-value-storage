package ru.andrey.kvstorage.initialization.impl;

import lombok.Builder;
import lombok.Getter;
import ru.andrey.kvstorage.index.Index;
import ru.andrey.kvstorage.index.SegmentIndexInfo;
import ru.andrey.kvstorage.initialization.SegmentInitializationContext;

import java.nio.file.Path;

@Getter
@Builder
public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final String segmentName;
    private final Path segmentPath;
    private final int currentSize;
    private final Index<String, SegmentIndexInfo> index; // todo sukhoa think of better design

    private SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, Index<String, SegmentIndexInfo> index) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, int currentSize) {
        this(segmentName, tablePath.resolve(segmentName), currentSize, null); // todo sukhoa maybe not null?
    }
}
