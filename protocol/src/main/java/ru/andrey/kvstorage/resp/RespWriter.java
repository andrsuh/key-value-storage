package ru.andrey.kvstorage.resp;

import ru.andrey.kvstorage.resp.object.RespObject;

import java.io.IOException;
import java.io.OutputStream;

public class RespWriter implements AutoCloseable{

    private final OutputStream os;

    public RespWriter(OutputStream os) {
        this.os = os;
    }

    /**
     * Записывает в output stream объект
     */
    public void write(RespObject object) throws IOException {
        object.write(os);
        os.flush();
    }

    @Override
    public void close() throws IOException {
        os.close();
    }
}
