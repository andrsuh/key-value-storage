package ru.andrey.kvstorage.logic;

import ru.andrey.kvstorage.exception.DatabaseException;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface Index {
    void update(String objectKey, IndexInfo info) throws DatabaseException;

    // think of something more suitable than IndexInfo for offset and segment info
    Optional<IndexInfo> searchForKey(String objectKey);

    // todo sukhoa redesign.
    Set<Map.Entry<String, IndexInfo>> values();
}