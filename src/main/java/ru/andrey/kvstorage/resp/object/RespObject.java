package ru.andrey.kvstorage.resp.object;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public interface RespObject {

    byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);

    boolean isError();

    String asString();

    void write(OutputStream os) throws IOException;

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
