package ru.andrey.kvstorage.resp.object;

import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import ru.andrey.kvstorage.resp.RespUtil;

import java.io.IOException;
import java.io.OutputStream;

@AllArgsConstructor
public class RespCommandId implements RespObject {

    public static final byte CODE = '!';

    public final int commandId;

    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public String asString() {
        return Integer.toString(commandId);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        RespUtil.writeInt(os, commandId);
        os.write(CRLF);
    }

    @Override
    public byte[] getPayloadBytes() {
        return Unpooled.buffer(4).writeInt(commandId).array();
    }

    @Override
    public String toString() {
        return "RespCommandId{" +
                "commandId=" + commandId +
                '}';
    }
}
