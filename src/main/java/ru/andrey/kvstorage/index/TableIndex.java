package ru.andrey.kvstorage.index;

import ru.andrey.kvstorage.logic.Segment;

import java.util.Optional;

public interface TableIndex { // todo sukhoa create generic interface
    Optional<Segment> searchForKey(String objectKey);

    void onTableUpdated(String objectKey, Segment segment);
}
