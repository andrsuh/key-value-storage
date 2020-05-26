package ru.andrey.kvstorage.resp;

import lombok.AllArgsConstructor;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.io.IOException;
import java.io.OutputStream;

@AllArgsConstructor
public class RespWriter {

    private final OutputStream os;

    /**
     * Записывает {@code object} в выходной поток.
     *
     * @param object объект, который нужно записать
     * @throws IOException если произошла ошибка ввода-вывода
     */
    public void write(RespObject object) throws IOException {
        object.write(os);
        os.flush();
    }
}
