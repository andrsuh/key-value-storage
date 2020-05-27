package ru.andrey.kvstorage.resp.object;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public interface RespObject {

    byte[] CRLF = "\r\n".getBytes(StandardCharsets.US_ASCII);
    byte[] MINUS_ONE = "-1".getBytes(StandardCharsets.US_ASCII);

    /**
     * Возвращает {@code true} - если данный объект является ошибкой, {@code false} - в ином случае.
     *
     * @return {@code true} - если данный объект является ошибкой, {@code false} - в ином случае
     */
    boolean isError();

    /**
     * Возвращает строковое представление данного объекта.
     *
     * @return строковое представление данного объекта
     */
    String asString();

    /**
     * Сериализует данный объект в байты и записывает его в OutputStream.
     *
     * @param os - выходной поток, куда будут записываться данные
     * @throws IOException если произошла ошибка ввода-вывода
     */
    void write(OutputStream os) throws IOException;

    /**
     * Возвращает байтовое представление данного объекта.
     *
     * @return байтовое представление данного объекта
     */
    default byte[] getBytes() {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();

        try {
            write(os);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return os.toByteArray();
    }
}
