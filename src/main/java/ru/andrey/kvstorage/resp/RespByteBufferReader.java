package ru.andrey.kvstorage.resp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import ru.andrey.kvstorage.resp.object.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class RespByteBufferReader {

    private static final byte CR = '\r';
    private static final byte LF = '\n';

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
                    return new RespSimpleStringReader(buf, RespSimpleString::new);
                case RespError.CODE:
                    return new RespSimpleStringReader(buf, RespError::new);
                case RespCommandId.CODE:
                    return new RespCommandIdReader(buf);
                case RespBulkString.CODE:
                    return new RespBulkStringReader(buf);
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

        protected void readEndBytes() {
            final byte cr = in.readByte();
            if (cr != CR) throw new IllegalArgumentException("Read something different from CR: " + cr);
            final byte lf = in.readByte();
            if (lf != LF) throw new IllegalArgumentException("Read something different from LF: " + lf);
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

    static class RespBulkStringReader extends RespStatefulReader {
        private static final Optional<RespBulkString> NULL_BULK_STRING = Optional.of(RespBulkString.NULL_BULK_STRING);

        public RespBulkStringReader(ByteBuf in) {
            super(in, RespStatefulReader.readInt(in));
        }

        @Override
        Optional<RespBulkString> readNextPortion() {
            if (size == RespBulkString.NULL_STRING_SIZE) {
                return NULL_BULK_STRING;
            }

            if (!in.isReadable(size)) {
                return Optional.empty();
            }

            final byte[] data = new byte[size];
            in.readBytes(data, 0, size);

            readEndBytes();

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

            readEndBytes();

            return Optional.of(new RespCommandId(res));
        }
    }

    static class RespSimpleStringReader extends RespStatefulReader {
        private final ByteBuf readSoFar = Unpooled.buffer(128);
        private final Function<byte[], RespObject> desiredTypeConstructor;

        public RespSimpleStringReader(ByteBuf in, Function<byte[], RespObject> desiredTypeConstructor) {
            super(in);
            this.desiredTypeConstructor = desiredTypeConstructor;
        }

        @Override
        Optional<RespObject> readNextPortion() {
            boolean aboutTheEnd = false;
            while (in.isReadable()) {
                byte readByte = in.readByte();

                if (aboutTheEnd) { // if "/r" byte was previously read
                    if (readByte == LF) { // and the current one is the "\n"
                        byte[] result = new byte[readSoFar.readableBytes()];
                        readSoFar.readBytes(result);

                        return Optional.of(desiredTypeConstructor.apply(result));
                    } else { // if it's not the end
                        aboutTheEnd = false; // cancel the flag
                        readSoFar.writeByte(CR); // and add previously read "\r" that was skipped before
                    }
                }

                if (readByte == CR) {
                    aboutTheEnd = true; // it might be the beginning of the end
                } else {
                    readSoFar.writeByte(readByte); // or just usual byte
                }
            }
            return Optional.empty();
        }
    }
}
