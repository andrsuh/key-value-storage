package ru.andrey.kvstorage.index;

import ru.andrey.kvstorage.logic.Segment;

@FunctionalInterface
public interface SegmentWriteListener {
    // todo sukhoa provide more info?
    // todo sukhoa investigate using of WeakRef
    void onWrite(Segment targetSegment, String objectKey);
}
