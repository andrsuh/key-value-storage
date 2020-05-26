package ru.andrey.kvstorage.server.initialization;


import ru.andrey.kvstorage.server.index.impl.SegmentIndex;

import java.nio.file.Path;

public interface SegmentInitializationContext {
    String getSegmentName();

    Path getSegmentPath();

    SegmentIndex getIndex();

    int getCurrentSize(); // todo sukhoa should be long
}
