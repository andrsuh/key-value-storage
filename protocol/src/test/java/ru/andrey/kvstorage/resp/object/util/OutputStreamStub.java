package ru.andrey.kvstorage.resp.object.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class OutputStreamStub extends OutputStream {
    private final StringBuilder stringBuilder = new StringBuilder();

    @Override
    public void write(byte[] b) throws IOException {
        stringBuilder.append(new String(b, StandardCharsets.UTF_8));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void write(int b) throws IOException {
        stringBuilder.append(new String(new byte[]{(byte) b}));
    }

    public String getString() {
        return stringBuilder.toString();
    }
}
