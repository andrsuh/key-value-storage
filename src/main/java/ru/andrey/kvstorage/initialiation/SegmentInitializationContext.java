package ru.andrey.kvstorage.initialiation;

import ru.andrey.kvstorage.logic.Index;

import java.nio.file.Path;

public interface SegmentInitializationContext {
    String getSegmentName();

    void setSegmentName(String segmentName);

    Path getSegmentPath();

    void setSegmentPath(Path segmentPath);

    int getCurrentSize(); // todo sukhoa should be long

    void setCurrentSize(int currentSize);

    Index getSegmentIndex();

    void setSegmentIndex(Index segmentIndex);
}
