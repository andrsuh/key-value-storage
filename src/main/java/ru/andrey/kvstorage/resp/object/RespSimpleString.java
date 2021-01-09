package ru.andrey.kvstorage.resp.object;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

@AllArgsConstructor
public class RespSimpleString implements RespObject {

    public static final byte CODE = '+';

    @Getter
    private final String string;

    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public String asString() {
        return string;
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(string.getBytes(StandardCharsets.UTF_8));
        os.write(CRLF);
    }
}
