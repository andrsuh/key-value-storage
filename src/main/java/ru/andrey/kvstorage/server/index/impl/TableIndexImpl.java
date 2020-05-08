package ru.andrey.kvstorage.server.index.impl;

import ru.andrey.kvstorage.server.index.TableIndex;
import ru.andrey.kvstorage.server.logic.Segment;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TableIndexImpl implements TableIndex {
    private final Map<String, Segment> index = new HashMap<>(); // todo sukhoa ConcurrentMap?

    @Override
    public Optional<Segment> searchForKey(String objectKey) {
        return Optional.ofNullable(index.get(objectKey));
    }

    @Override
    public void onTableUpdated(String objectKey, Segment segment) {
        Segment previous = index.put(objectKey, segment);
        if (previous != null && segment != previous) { // todo sukhoa override equals
            System.out.println("key: " + objectKey + " segment: " + segment.getName());
        }
    }
}
