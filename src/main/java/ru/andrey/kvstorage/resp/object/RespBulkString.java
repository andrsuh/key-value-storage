package ru.andrey.kvstorage.resp.object;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import ru.andrey.kvstorage.resp.RespUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

@AllArgsConstructor
@NoArgsConstructor
public class RespBulkString implements RespObject {
    public static final int MINUS_ONE = -1;

    public static final byte CODE = '$';

    private byte[] data;

    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public String asString() {
        if (data != null) {
            return new String(data, StandardCharsets.UTF_8);
        } else {
            return null;
        }
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        if (data == null) {
            RespUtil.writeInt(os, MINUS_ONE);
        } else {
            RespUtil.writeInt(os, data.length);
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
