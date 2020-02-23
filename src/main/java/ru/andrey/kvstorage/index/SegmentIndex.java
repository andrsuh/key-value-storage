package ru.andrey.kvstorage.index;

import java.util.Optional;

public interface SegmentIndex { // todo sukhoa create generic interface
    Optional<SegmentIndexInfo> searchForKey(String objectKey);

    void onSegmentUpdated(String objectKey, SegmentIndexInfo indexInfo);
}
