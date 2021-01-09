package ru.andrey.kvstorage.resp.object;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import ru.andrey.kvstorage.jclient.command.KvsCommand;
import ru.andrey.kvstorage.resp.RespUtil;

import java.io.IOException;
import java.io.OutputStream;

@AllArgsConstructor
@NoArgsConstructor
public class RespCommandId implements RespObject {

    public static final byte CODE = '!';

    public int commandId = KvsCommand.idGen.getAndIncrement();

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
    public String toString() {
        return "RespCommandId{" +
                "commandId=" + commandId +
                '}';
    }
}
