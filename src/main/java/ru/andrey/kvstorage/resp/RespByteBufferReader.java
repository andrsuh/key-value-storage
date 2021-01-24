package ru.andrey.kvstorage.resp;

import io.netty.buffer.ByteBuf;
import ru.andrey.kvstorage.resp.object.*;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RespByteBufferReader {

    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static final byte MINUS = '-';
    private static final byte ZERO = '0';

    private final ByteBuf buf;

    private RespStatefulReader nextReader;

    public RespByteBufferReader(ByteBuf buf) {
        this.buf = buf;
    }

    public Optional<? extends RespObject> readObject() throws IOException {
        if (nextReader == null) {
            nextReader = RespStatefulReader.getNextReader(buf);
        }
        Optional<? extends RespObject> respObject = nextReader.readNextPortion();
        if (respObject.isPresent()) {
            nextReader = null;
        }
        return respObject;
    }

    //fkats: this code linked with RespReader. Is it okay?
    static abstract class RespStatefulReader {
        protected int size;
        protected ByteBuf in;

        public RespStatefulReader(ByteBuf in) {
            this(in, 0);
        }

        public RespStatefulReader(ByteBuf in, int size) {
            this.size = size;
            this.in = in;
        }

        abstract Optional<? extends RespObject> readNextPortion() throws IOException;

        public static RespStatefulReader getNextReader(ByteBuf buf) {
            if (!buf.isReadable()) {
                throw new IllegalArgumentException("Unexpected end of stream");
            }

            byte leadingByte = buf.readByte();

            switch (leadingByte) {
                case RespSimpleString.CODE:
                    return new RespSimpleStringReader(buf);
                case RespError.CODE:
                    return new RespErrorReader(buf);
                case RespCommandId.CODE:
                    return new RespCommandIdReader(buf);
                case RespBulkString.CODE:
                    return new RespStringReader(buf);
                case RespArray.CODE:
                    return new RespArrayReader(buf);
                default:
                    throw new IllegalArgumentException("Unknown type character in stream:");
            }
        }

        private static int readInt(ByteBuf in) {
            int res = in.readInt();
            in.readByte();
            in.readByte();
            return res;
        }
    }

    static class RespArrayReader extends RespStatefulReader {
        private RespStatefulReader nextElementReader;
        private final List<RespObject> parsed = new ArrayList<>(8);

        public RespArrayReader(ByteBuf in) {
            super(in, RespStatefulReader.readInt(in));
        }

        @Override
        Optional<RespArray> readNextPortion() throws IOException {
            while (parsed.size() < size && in.isReadable()) {
                if (nextElementReader == null) {
                    nextElementReader = RespStatefulReader.getNextReader(in);
                }

                Optional<? extends RespObject> respObject = nextElementReader.readNextPortion();
                if (respObject.isPresent()) {
                    parsed.add(respObject.get());
                    nextElementReader = null;
                } else {
                    return Optional.empty();
                }
            }

            if (parsed.size() == size) {
                return Optional.of(new RespArray(parsed));
            }

            return Optional.empty();
        }
    }

    static class RespStringReader extends RespStatefulReader {
        public RespStringReader(ByteBuf in) {
            super(in, RespStatefulReader.readInt(in));
        }

        @Override
        Optional<RespBulkString> readNextPortion() {
            if (!in.isReadable(size)) {
                return Optional.empty();
            }

            final byte[] data = new byte[size];
            in.readBytes(data, 0, size);

            final byte cr = in.readByte();
            final byte lf = in.readByte();

            return Optional.of(new RespBulkString(data));
        }
    }

    static class RespCommandIdReader extends RespStatefulReader {
        public RespCommandIdReader(ByteBuf in) {
            super(in);
        }

        @Override
        Optional<RespCommandId> readNextPortion() {
            if (!in.isReadable(size)) {
                return Optional.empty();
            }

            int res = in.readInt();

            final byte cr = in.readByte();
            final byte lf = in.readByte();

            return Optional.of(new RespCommandId(res));
        }
    }

    static class RespErrorReader extends  RespStatefulReader {
        public RespErrorReader(ByteBuf in) {
            super(in);
        }

        @Override
        Optional<RespError> readNextPortion() throws IOException {
            if (!in.isReadable(size)) {
                return Optional.empty();
            }

            String result = readString(in);
            return Optional.of(new RespError(result.getBytes(StandardCharsets.UTF_8)));
        }
    }

    static class RespSimpleStringReader extends  RespStatefulReader {
        public RespSimpleStringReader(ByteBuf in) {
            super(in, RespStatefulReader.readInt(in));
        }

        @Override
        Optional<RespSimpleString> readNextPortion() throws IOException {
            if (!in.isReadable(size)) {
                return Optional.empty();
            }

            String result = readString(in);
            return Optional.of(new RespSimpleString(result));
        }
    }

    private static String readString(ByteBuf is) throws IOException {
        byte[] buf = new byte[128];
        int room = buf.length;
        int count = 0;
        byte b = is.readByte();

        while (true) {
            if (b == -1) {
                throw new EOFException("Unexpected end of stream");
            }

            if (b == CR) {
                final int b1 = is.readByte();
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

            buf[count++] = b;
            b = is.readByte();
        }

        return new String(buf, 0, count, StandardCharsets.UTF_8);
    }
}
