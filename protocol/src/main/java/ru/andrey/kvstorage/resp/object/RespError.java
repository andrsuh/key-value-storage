package ru.andrey.kvstorage.resp.object;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Сообщение об ошибке в RESP протоколе
 */
@AllArgsConstructor
public class RespError implements RespObject {

    public static final byte CODE = '-';

    @Getter
    private final byte[] message;

    @Override
    public boolean isError() {
        return true;
    }

    @Override
    public String asString() {
        return new String(message, StandardCharsets.UTF_8);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(message);
        os.write(CRLF);
    }
}
