package ru.andrey.kvstorage.index;

import java.util.Optional;

public interface Index<K, V> {
    Optional<V> getIndex(K objectKey);
    void updateIndex(K objectKey, V indexInfo);
}
