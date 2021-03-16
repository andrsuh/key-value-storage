package ru.andrey.kvstorage.server.logic.io;

import ru.andrey.kvstorage.server.logic.WritableDatabaseRecord;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Записывает данные в БД
 */
public class DatabaseOutputStream extends DataOutputStream {

    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    /**
     * Записывает в БД в следующем формате:
     * - Размер ключа в байтахб используя {@link WritableDatabaseRecord#getKeySize()}
     * - Ключ
     * - Размер записи в байтах (для null-значений размер равен -1) {@link WritableDatabaseRecord#getValueSize()}
     * - Запись
     * Например при использовании UTF_8,
     * "key" : "value"
     * 3key5value
     * Метод вернет 10
     *
     * @param databaseRecord запись
     * @return размер записи
     * @throws IOException если запись не удалась
     */
    public int write(WritableDatabaseRecord databaseRecord) throws IOException {
        int sizeBefore = size();
        writeInt(databaseRecord.getKeySize());
        write(databaseRecord.getKey());
        writeInt(databaseRecord.getValueSize());
        if (databaseRecord.getValue() != null)
            write(databaseRecord.getValue());

        return size() - sizeBefore;
    }
}
