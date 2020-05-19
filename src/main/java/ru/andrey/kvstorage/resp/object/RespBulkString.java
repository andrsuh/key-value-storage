package ru.andrey.kvstorage.resp.object;

import lombok.AllArgsConstructor;
import lombok.Getter;
import ru.andrey.kvstorage.resp.RespUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

@AllArgsConstructor
public class RespBulkString implements RespObject {

    public static final byte CODE = '$';

    @Getter
    private final byte[] data;

    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public String asString() {
        return new String(data, StandardCharsets.US_ASCII);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        if (data == null) {
            os.write(MINUS_ONE);
        } else {
            RespUtil.writeInt(os, data.length);
            os.write(CRLF);
            os.write(data);
        }
        os.write(CRLF);
    }
}
