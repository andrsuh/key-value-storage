package ru.andrey.kvstorage.resp;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class RespUtil {

    /**
     * Записывает число в {@code os} в символьно-цифровом представлении.
     *
     * @param os входной поток, куда нужно записать число
     * @param number число, которое нужно записать
     * @throws IOException если произошла ошибка ввода-вывода
     */
    public static void writeInt(OutputStream os, int number) throws IOException {
        os.write(Integer.toString(number).getBytes(StandardCharsets.US_ASCII));
    }
}
