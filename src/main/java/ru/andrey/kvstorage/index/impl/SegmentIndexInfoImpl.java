package ru.andrey.kvstorage.index.impl;

import ru.andrey.kvstorage.index.SegmentIndexInfo;

public class SegmentIndexInfoImpl implements SegmentIndexInfo {
    private final long offset;
    private final long length; // TODO A: won't integers suffice?

    public SegmentIndexInfoImpl(long offset, long length) {
        this.offset = offset;
        this.length = length;
    }

    @Override
    public long getOffset() {
        return this.offset;
    }

    @Override
    public long getLength() {
        return this.length;
    }
}
