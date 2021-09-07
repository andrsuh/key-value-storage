package ru.andrey.kvstorage.resp.object.util;

public class IntConverter {
    public static byte[] intToBytes(int value) {
        return new byte[] {
                (byte)(value >>> 24),
                (byte)(value >>> 16),
                (byte)(value >>> 8),
                (byte)value
        };
    }
}
