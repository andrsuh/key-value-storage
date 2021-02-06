package ru.andrey.kvstorage.resp;

import lombok.AllArgsConstructor;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.io.IOException;
import java.io.OutputStream;

@AllArgsConstructor
public class RespWriter {

    private final OutputStream os;

    public void write(RespObject object) throws IOException {
        object.write(os);
        os.flush();
    }
}
