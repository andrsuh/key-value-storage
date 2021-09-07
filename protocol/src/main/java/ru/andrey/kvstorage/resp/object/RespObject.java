package ru.andrey.kvstorage.resp.object;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Преставляет собой объект в RESP
 */
public interface RespObject {

    /**
     * Последовательность символов, означающая конец объекта
     */
    byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);

    /**
     * @return {@code true} - если данный объект представляет ошибку, {@code false} - в ином случае
     */
    boolean isError();

    /**
     * @return возвращает строковое значение команды (не в RESP, без специальных символов).
     * Например, для {@link RespBulkString} со значением "string" - "string"
     */
    String asString();

    /**
     * Сериализует данный объект в RESP и записывает байты в переданный OutputStream.
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
            e.printStackTrace(); // todo sukhoa baaaaaaad
            return null;
        }

        return os.toByteArray();
    }
}