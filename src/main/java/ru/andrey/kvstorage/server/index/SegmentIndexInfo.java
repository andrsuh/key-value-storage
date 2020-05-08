package ru.andrey.kvstorage.server.index;

public interface SegmentIndexInfo {
    long getOffset();

    long getLength();
}
