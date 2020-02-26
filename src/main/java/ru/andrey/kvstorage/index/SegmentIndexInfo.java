package ru.andrey.kvstorage.index;
// TODO A: should it really be an interface?
public interface SegmentIndexInfo {
    long getOffset();

    long getLength();
}
