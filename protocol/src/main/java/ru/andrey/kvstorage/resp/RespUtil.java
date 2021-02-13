package ru.andrey.kvstorage.resp;

import java.io.IOException;
import java.io.OutputStream;

public class RespUtil {

    public static void writeInt(OutputStream os, int number) throws IOException {
        os.write(intToByteArray(number));
    }

    public static byte[] intToByteArray(int value) {
        return new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value};
    }
}
