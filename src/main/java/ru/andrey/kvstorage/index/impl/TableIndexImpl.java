package ru.andrey.kvstorage.index.impl;

import ru.andrey.kvstorage.index.TableIndex;
import ru.andrey.kvstorage.logic.Segment;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TableIndexImpl implements TableIndex {
    private final Map<String, Segment> index = new HashMap<>(); // todo sukhoa ConcurrentMap

    @Override
    public Optional<Segment> searchForKey(String objectKey) {
        return Optional.ofNullable(index.get(objectKey));
    }

    @Override
    public void onTableUpdated(String objectKey, Segment segment) {
        index.put(objectKey, segment);
    }
}
