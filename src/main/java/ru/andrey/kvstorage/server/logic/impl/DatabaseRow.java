package ru.andrey.kvstorage.server.logic.impl;

public class DatabaseRow {
    private final byte[] key;
    private final int keySize;

    private final byte[] value;
    private final int valueSize;

    public DatabaseRow(String objectKey, String objectValue) {
        this(objectKey.getBytes(), objectValue.getBytes());
    }

    public DatabaseRow(byte[] key, byte[] value) {
        this.key = key;
        keySize = key.length;
        this.value = value;
        valueSize = value.length;
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
        return Integer.BYTES + getKeySize() + getValueSize();
    }

}