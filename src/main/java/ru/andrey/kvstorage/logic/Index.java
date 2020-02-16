package ru.andrey.kvstorage.logic;

import ru.andrey.kvstorage.exception.DatabaseException;

import java.util.Optional;

public interface Index {
    void update(String objectKey, IndexInfo info) throws DatabaseException;

    // think of something more suitable than IndexInfo for offset and segment info
    Optional<IndexInfo> searchForKey(String objectKey);
}