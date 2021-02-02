package ru.andrey.kvstorage.server.index.impl;

import ru.andrey.kvstorage.server.index.SegmentOffsetInfo;

public class SegmentOffsetInfoImpl implements SegmentOffsetInfo {
    private final long offset;

    public SegmentOffsetInfoImpl(long offset) {
        this.offset = offset;
    }

    @Override
    public long getOffset() {
        return this.offset;
    }
}
