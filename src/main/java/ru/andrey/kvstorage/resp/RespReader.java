package ru.andrey.kvstorage.resp;

import lombok.AllArgsConstructor;
import ru.andrey.kvstorage.resp.object.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@AllArgsConstructor
public class RespReader {

    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static final byte MINUS = '-';
    private static final byte ZERO = '0';

    private final InputStream is;

    public boolean hasArray() throws IOException {
        return is.read() == RespArray.CODE;
    }

    public RespObject readObject() throws IOException {
        final int code = is.read();
        if (code == -1) {
            throw new EOFException("Unexpected end of stream");
        }

        switch (code) {
            case RespSimpleString.CODE:
                return readSimpleString();
            case RespError.CODE:
                return readError();
            case RespBulkString.CODE:
                return readBulkString();
            case RespArray.CODE:
                return readArray();
            default:
                throw new IOException(String.format("Unknown type character in stream: %1$x", code));
        }
    }

    public RespSimpleString readSimpleString() throws IOException {
        return new RespSimpleString(readString());
    }

    public RespError readError() throws IOException {
        return new RespError(readString());
    }

    public RespBulkString readBulkString() throws IOException {
        final int size = readInt();

        if (size == -1) {
            return new RespBulkString(null);
        }
        if (size < 0) {
            throw new IOException(String.format("Invalid bulk string size: %1$d", size));
        }

        final byte[] data = new byte[size];
        final int read = is.read(data, 0, size);

        if (read == -1) {
            throw new EOFException("Unexpected end of stream");
        }
        if (read != size) {
            throw new IOException(String.format("Failed to read enough chars. Read: %1$d, Expected: %2$d", read, size));
        }

        final int cr = is.read();
        final int lf = is.read();

        if (cr == -1 || lf == -1) {
            throw new EOFException("Unexpected end of stream");
        }
        if (cr != CR || lf != LF) {
            throw new IOException(String.format("Unexpected line ending of bulk string: %1$x, %2$x", cr, lf));
        }

        return new RespBulkString(data);
    }

    public RespArray readArray() throws IOException {
        final int size = readInt();

        if (size < 0) {
            throw new IOException(String.format("Invalid array size: %1$d", size));
        }

        final RespObject[] objects = new RespObject[size];
        for (int i = 0; i < size; ++i) {
            objects[i] = readObject();
        }

        return new RespArray(objects);
    }

    private String readString() throws IOException {
        byte[] buf = new byte[128];
        int room = buf.length;
        int count = 0;
        int b = is.read();

        while (true) {
            if (b == -1) {
                throw new EOFException("Unexpected end of stream");
            }

            if (b == CR) {
                final int b1 = is.read();
                if (b1 == -1) {
                    throw new EOFException("Unexpected end of stream");
                }
                if (b1 != LF) {
                    throw new EOFException(String.format("Unexpected character after CR: %1$x", b1));
                }
                break;
            }

            if (--room < 0) {
                final byte[] prevBuf = buf;
                buf = new byte[count + 128];
                room = buf.length - count - 1;
                System.arraycopy(prevBuf, 0, buf, 0, count);
            }

            buf[count++] = (byte) b;
            b = is.read();
        }

        return new String(buf, 0, count, StandardCharsets.UTF_8);
    }

    private int readInt() throws IOException {
        final int sign;

        int b = is.read();
        if (b == MINUS) {
            b = is.read();
            sign = -1;
        } else {
            sign = 1;
        }

        int number = 0;
        while (true) {
            if (b == -1) {
                throw new EOFException("Unexpected end of stream");
            }

            if (b == CR) {
                if (is.read() == LF) {
                    return sign * number;
                }
                throw new IOException("Invalid character in integer");
            }

            final int digit = b - ZERO;
            if (digit < 0 || 10 <= digit) {
                throw new IOException("Invalid character in integer");
            }

            number = number * 10 + digit;
            b = is.read();
        }
    }
}
