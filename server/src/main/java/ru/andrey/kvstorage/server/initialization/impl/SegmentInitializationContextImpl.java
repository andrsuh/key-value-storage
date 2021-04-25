package ru.andrey.kvstorage.server.initialization.impl;

import lombok.Builder;
import lombok.Getter;
import ru.andrey.kvstorage.server.exception.DatabaseException;
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

    /**
     * Не используйте этот конструктор. Оставлен для совместимости со старыми тестами.
     */
    public SegmentInitializationContextImpl(String segmentName, Path tablePath, long currentSize) {
        this(segmentName, tablePath.resolve(segmentName), currentSize, new SegmentIndex());
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath) {
        this(segmentName, tablePath.resolve(segmentName), 0, new SegmentIndex());
    }
}
