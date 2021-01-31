package ru.andrey.kvstorage.server.logic.impl;

import ru.andrey.kvstorage.server.logic.WritableDatabaseRecord;

/**
 * Запись в БД, означающая добавление значения по ключу
 */
public class SetDatabaseRecord implements WritableDatabaseRecord {
    private final byte[] key;
    private final int keySize;

    private final byte[] value;
    private final int valueSize;

    public SetDatabaseRecord(String objectKey, byte[] objectValue) {
        this(objectKey.getBytes(), objectValue);
    }

    public SetDatabaseRecord(byte[] key, byte[] value) {
        this.key = key;
        this.keySize = key.length;
        this.value = value;
        this.valueSize = value.length;
    }

    public byte[] getKey() {
        return key;
    }

    public int getKeySize() {
        return keySize;
    }

    public byte[] getValue() {
        return value;
    }

    public int getValueSize() {
        return valueSize;
    }

    public long size() {
        return Integer.BYTES + getKeySize() + Integer.BYTES + getValueSize();
    }

    @Override
    public boolean isValuePresented() {
        return true;
    }

}
