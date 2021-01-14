package ru.andrey.kvstorage.server.logic.impl;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public class DatabaseInputStream extends DataInputStream {
    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    public Optional<DatabaseRow> readDbUnit() throws IOException {
        try {
            int keySize = readInt();
            byte[] key = readNBytes(keySize);

            int valueSize = readInt();
            byte[] value = readNBytes(valueSize);

            return Optional.of(new DatabaseRow(key, value));
        } catch (EOFException e) {
            return Optional.empty();
        }
    }
}
