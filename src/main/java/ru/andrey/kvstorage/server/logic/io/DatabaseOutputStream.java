package ru.andrey.kvstorage.server.logic.io;

import ru.andrey.kvstorage.server.logic.impl.DatabaseRow;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DatabaseOutputStream extends DataOutputStream {
    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    public int write(DatabaseRow databaseRow) throws IOException {
        int sizeBefore = size();
        writeInt(databaseRow.getKeySize());
        write(databaseRow.getKey());
        writeInt(databaseRow.getValueSize());
        write(databaseRow.getValue());
        return size() - sizeBefore;
    }
}
