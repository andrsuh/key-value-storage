package ru.andrey.kvstorage.logic;

import ru.andrey.kvstorage.DatabaseException;

import java.io.IOException;

public interface Segment {

    String getName();

    // returns offset but maybe it's worth returning the offset and the number of bytes written?
    // exception is also questionable
    int write(String objectKey, String objectValue) throws IOException, DatabaseException;

    String read(String objectKey) throws IOException;
}