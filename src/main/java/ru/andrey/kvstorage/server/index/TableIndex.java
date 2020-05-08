package ru.andrey.kvstorage.server.index;

import ru.andrey.kvstorage.server.logic.Segment;

import java.util.Optional;

public interface TableIndex { // todo sukhoa create generic interface
    Optional<Segment> searchForKey(String objectKey);

    void onTableUpdated(String objectKey, Segment segment);
}
