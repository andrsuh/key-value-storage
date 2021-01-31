package ru.andrey.kvstorage.server.logic;

import ru.andrey.kvstorage.server.exception.DatabaseException;

import java.io.IOException;
import java.util.Optional;

public interface Segment {

    String getName();

    // todo sukhoa in future may return something like SegmentWriteResult .. with report and error details?
    // for new returns false if cannot allocate requested capacity
    // exception is questionable
    boolean write(String objectKey, byte[] objectValue) throws IOException, DatabaseException;

    Optional<byte[]> read(String objectKey) throws IOException;

    boolean isReadOnly();

    boolean delete(String objectKey) throws IOException;
}