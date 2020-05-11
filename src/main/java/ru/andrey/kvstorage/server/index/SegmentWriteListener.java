package ru.andrey.kvstorage.server.index;

import ru.andrey.kvstorage.server.logic.Segment;

@FunctionalInterface
public interface SegmentWriteListener {
    // todo sukhoa provide more info?
    // todo sukhoa investigate using of WeakRef
    void onWrite(Segment targetSegment, String objectKey);
}
