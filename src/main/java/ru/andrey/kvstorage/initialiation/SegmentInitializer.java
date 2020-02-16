package ru.andrey.kvstorage.initialiation;

import ru.andrey.kvstorage.exception.DatabaseException;

public interface SegmentInitializer {

    void prepareContext(SegmentInitializationContext context) throws DatabaseException;
}
