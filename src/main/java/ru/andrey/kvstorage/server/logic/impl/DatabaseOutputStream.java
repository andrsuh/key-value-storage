package ru.andrey.kvstorage.server.logic.impl;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DatabaseOutputStream extends DataOutputStream {
    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    int write(DatabaseStoringUnit storingUnit) throws IOException {
        int sizeBefore = size();
        writeInt(storingUnit.getKeySize());
        write(storingUnit.getKey());
        writeInt(storingUnit.getValueSize());
        write(storingUnit.getValue());
        return size() - sizeBefore;
    }
}
