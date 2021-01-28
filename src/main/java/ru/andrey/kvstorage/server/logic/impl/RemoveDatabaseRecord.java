package ru.andrey.kvstorage.server.logic.impl;

import ru.andrey.kvstorage.server.logic.WritableDatabaseRecord;

/**
 * Запись в БД, означающая удаление значения по ключу
 */
public class RemoveDatabaseRecord implements WritableDatabaseRecord {
    private static final byte[] EMPTY_VALUE = new byte[0];

    private final byte[] keyValue;
    private final int keySize;

    public RemoveDatabaseRecord(String key) {
        this(key.getBytes());
    }

    public RemoveDatabaseRecord(byte[] keyValue) {
        this.keyValue = keyValue;
        this.keySize = keyValue.length;
    }

    @Override
    public byte[] getKey() {
        return keyValue;
    }

    @Override
    public int getKeySize() {
        return keySize;
    }

    @Override
    public byte[] getValue() {
        return EMPTY_VALUE;
    }

    @Override
    public int getValueSize() {
        return -1;
    }

    @Override
    public long size() {
        return Integer.BYTES + getKeySize() + Integer.BYTES;
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }
}
