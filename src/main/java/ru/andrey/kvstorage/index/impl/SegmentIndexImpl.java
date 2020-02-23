package ru.andrey.kvstorage.index.impl;

import ru.andrey.kvstorage.index.SegmentIndex;
import ru.andrey.kvstorage.index.SegmentIndexInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SegmentIndexImpl implements SegmentIndex {
    private final Map<String, SegmentIndexInfo> index = new HashMap<>(100); // todo sukhoa fix magic constant


    @Override
    public Optional<SegmentIndexInfo> searchForKey(String objectKey) {
        return Optional.ofNullable(index.get(objectKey));
    }

    @Override
    public void onSegmentUpdated(String objectKey, SegmentIndexInfo indexInfo) {
        index.put(objectKey, indexInfo);
    }
}
