package ru.andrey.kvstorage.server.index;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MapBasedKvsIndex<K, V> implements KvsIndex<K, V> {
    private final Map<K, V> index = new HashMap<>(200);

    @Override
    public void onIndexedEntityUpdated(K key, V value) {
        index.put(key, value);
    }

    @Override
    public Optional<V> searchForKey(K key) {
        return Optional.ofNullable(index.get(key));
    }
}
