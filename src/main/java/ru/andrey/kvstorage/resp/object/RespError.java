package ru.andrey.kvstorage.resp.object;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

@AllArgsConstructor
public class RespError implements RespObject {

    public static final byte CODE = '-';

    @Getter
    private final String message;

    @Override
    public boolean isError() {
        return true;
    }

    @Override
    public String asString() {
        return message;
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(message.getBytes(StandardCharsets.UTF_8));
        os.write(CRLF);
    }
}
