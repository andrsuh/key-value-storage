package ru.andrey.kvstorage.server.index.impl;

import ru.andrey.kvstorage.server.index.SegmentIndexInfo;

public class SegmentIndexInfoImpl implements SegmentIndexInfo {
    private final long offset;

    public SegmentIndexInfoImpl(long offset) {
        this.offset = offset;
    }

    @Override
    public long getOffset() {
        return this.offset;
    }
}
