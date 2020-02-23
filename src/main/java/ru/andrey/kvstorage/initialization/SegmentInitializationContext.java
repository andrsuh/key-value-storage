package ru.andrey.kvstorage.initialization;

import ru.andrey.kvstorage.index.SegmentIndex;

import java.nio.file.Path;

public interface SegmentInitializationContext {
    String getSegmentName();

    Path getSegmentPath();

    SegmentIndex getIndex();

    int getCurrentSize(); // todo sukhoa should be long
}
