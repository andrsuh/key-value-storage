package ru.andrey.kvstorage.server.logic.io;

import ru.andrey.kvstorage.server.logic.DatabaseRecord;
import ru.andrey.kvstorage.server.logic.WritableDatabaseRecord;
import ru.andrey.kvstorage.server.logic.impl.RemoveDatabaseRecord;
import ru.andrey.kvstorage.server.logic.impl.SetDatabaseRecord;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Класс, отвечающий за чтение данных из БД
 */
public class DatabaseInputStream extends DataInputStream {
    private static final int REMOVED_OBJECT_SIZE = -1;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit() throws IOException {
        try {
            int keySize = readInt();
            byte[] key = readNBytes(keySize);

            int valueSize = readInt();

            if (valueSize == REMOVED_OBJECT_SIZE) {
                return Optional.of(new RemoveDatabaseRecord(key));
            } else {
                byte[] value = readNBytes(valueSize);
                return Optional.of(new SetDatabaseRecord(key, value));
            }

        } catch (EOFException e) {
            return Optional.empty();
        }
    }
}
