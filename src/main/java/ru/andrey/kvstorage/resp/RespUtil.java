package ru.andrey.kvstorage.resp;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class RespUtil {

    public static void writeInt(OutputStream os, int number) throws IOException {
        os.write(Integer.toString(number).getBytes(StandardCharsets.US_ASCII));
    }
}
