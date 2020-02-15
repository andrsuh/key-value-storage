package ru.andrey.kvstorage.logic;

import ru.andrey.kvstorage.DatabaseException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SegmentIndex implements Index {
    private final Map<String, IndexInfo> index = new HashMap<>(100); // todo sukhoa fix magic constant

    @Override
    public void update(String objectKey, IndexInfo info) throws DatabaseException {
        index.put(objectKey, info);
    }

    @Override
    public Optional<IndexInfo> searchForKey(String objectKey) {
        return Optional.ofNullable(index.get(objectKey));
    }
}
