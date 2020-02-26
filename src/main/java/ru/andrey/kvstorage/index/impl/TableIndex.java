package ru.andrey.kvstorage.index.impl;

import ru.andrey.kvstorage.index.Index;
import ru.andrey.kvstorage.logic.Segment;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TableIndex<K, S extends Segment> implements Index<K, S> {
    private final Map<K, S> index = new HashMap<>(); // todo sukhoa ConcurrentMap?

    @Override
    public Optional<S> getIndex(K objectKey) {
        return Optional.ofNullable(index.get(objectKey));
    }

    @Override
    public void updateIndex(K objectKey, S segment) {
        S previous = index.put(objectKey, segment);
        if (previous != null && segment != previous) { // todo sukhoa override equals
            System.out.println("key: " + objectKey + " segment: " + segment.getName());
        }
    }
}
