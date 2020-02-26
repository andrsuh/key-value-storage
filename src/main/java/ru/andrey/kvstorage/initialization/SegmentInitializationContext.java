package ru.andrey.kvstorage.initialization;

import ru.andrey.kvstorage.index.Index;
import ru.andrey.kvstorage.index.SegmentIndexInfo;

import java.nio.file.Path;

public interface SegmentInitializationContext {
    String getSegmentName();

    Path getSegmentPath();

    Index<String, SegmentIndexInfo> getIndex();

    int getCurrentSize(); // todo sukhoa should be long
}
