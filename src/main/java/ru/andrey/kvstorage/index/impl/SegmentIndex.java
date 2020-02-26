package ru.andrey.kvstorage.index.impl;

import ru.andrey.kvstorage.index.Index;
import ru.andrey.kvstorage.index.SegmentIndexInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SegmentIndex<K, S extends SegmentIndexInfo> implements Index<K, S> {
    /**
     * This value was found out experimentally (not)
     */
    private static final int INITIAL_IDX_COUNT = 64;
    private final Map<K, S> index = new HashMap<>(INITIAL_IDX_COUNT);

    @Override
    public Optional<S> getIndex(K objectKey) {
        return Optional.ofNullable(index.get(objectKey));
    }

    @Override
    public void updateIndex(K objectKey, S indexInfo) {
        index.put(objectKey, indexInfo);
    }
}
