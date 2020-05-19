package ru.andrey.kvstorage.resp.object;

import lombok.Getter;
import lombok.NonNull;
import ru.andrey.kvstorage.resp.RespUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.stream.Collectors;

public class RespArray implements RespObject {

    public static final byte CODE = '*';

    private static final String AS_STRING_SEPARATOR = " ";

    @Getter
    @NonNull
    private final RespObject[] objects;

    public RespArray(RespObject... objects) {
        this.objects = objects;
    }

    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public String asString() {
        return Arrays.stream(objects)
            .map(RespObject::asString)
            .collect(Collectors.joining(AS_STRING_SEPARATOR));
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        RespUtil.writeInt(os, objects.length);
        os.write(CRLF);

        for (RespObject object : objects) {
            object.write(os);
        }
    }
}
