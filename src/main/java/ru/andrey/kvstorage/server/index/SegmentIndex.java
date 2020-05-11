package ru.andrey.kvstorage.server.index;

import java.util.Optional;

public interface SegmentIndex { // todo sukhoa create generic interface
    Optional<SegmentIndexInfo> searchForKey(String objectKey);

    void onSegmentUpdated(String objectKey, SegmentIndexInfo indexInfo);
}
