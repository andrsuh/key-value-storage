package ru.andrey.kvstorage.logic;

public class IndexInfoImpl implements IndexInfo {
    private final long offset;
    private final long length;

    public IndexInfoImpl(long offset, long length) {
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
