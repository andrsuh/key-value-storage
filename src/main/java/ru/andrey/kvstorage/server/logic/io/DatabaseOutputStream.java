package ru.andrey.kvstorage.server.logic.io;

import ru.andrey.kvstorage.server.logic.impl.DatabaseRow;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DatabaseOutputStream extends DataOutputStream {
    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    public int write(DatabaseRow storingUnit) throws IOException {
        int sizeBefore = size();
        writeInt(storingUnit.getKeySize());
        write(storingUnit.getKey());
        writeInt(storingUnit.getValueSize());
        write(storingUnit.getValue());
        return size() - sizeBefore;
    }
}
