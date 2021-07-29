package ru.andrey.kvstorage.resp.object;

import lombok.AllArgsConstructor;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Строка
 */
@AllArgsConstructor
public class RespBulkString implements RespObject {
    /**
     * Код объекта
     */
    public static final byte CODE = '$';

    public static final int NULL_STRING_SIZE = -1;

    public static final RespBulkString NULL_STRING = new RespBulkString(null);

    private final byte[] data;

    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public String asString() {
        if (data != null) {
            return new String(data, StandardCharsets.UTF_8); // todo sukhoa get charset from settingsw
        }
        return null;
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);

        if (data == null) {
            os.write(String.valueOf(NULL_STRING_SIZE).getBytes(StandardCharsets.UTF_8));
        } else {
            os.write(String.valueOf(data.length).getBytes(StandardCharsets.UTF_8));
            os.write(CRLF);
            os.write(data);
        }

        os.write(CRLF);
    }

    @Override
    public String toString() {
        return "RespBulkString{" +
                "content=" + this.asString() +
                '}';
    }
}
