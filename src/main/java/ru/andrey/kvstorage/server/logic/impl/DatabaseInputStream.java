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

    /**
     * Считывает из входного потока {@code DatabaseStorageUnit} и возвращает его в виде {@code Optional<DatabaseStorageUnit>}.
     *
     * @return {@code Optional<DatabaseStorageUnit>}
     * @throws IOException если произошла ошибка ввода-вывода.
     */
    public Optional<DatabaseStoringUnit> readDbUnit() throws IOException {
        try {
            int keySize = readInt();
            byte[] key = readNBytes(keySize);

            int valueSize = readInt();
            byte[] value = readNBytes(valueSize);

            return Optional.of(new DatabaseStoringUnit(key, value));
        } catch (EOFException e) {
            return Optional.empty();
        }
    }
}
