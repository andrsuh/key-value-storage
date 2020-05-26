package ru.andrey.kvstorage.server.resp;

import lombok.AllArgsConstructor;
import ru.andrey.kvstorage.resp.RespReader;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;

import java.io.IOException;
import java.util.Arrays;

@AllArgsConstructor
public class CommandReader {

    private final RespReader reader;
    private final ExecutionEnvironment env;

    /**
     * Возвращает {@code true} - если во входном потоке есть команда, {@code false} - в ином случае.
     *
     * @return {@code true} - если во входном потоке есть команда, {@code false} - в ином случае
     * @throws IOException если произошла ошибка ввода-вывода.
     */
    public boolean hasNextCommand() throws IOException {
        return reader.hasArray();
    }

    /**
     * Принудительно пытается считать {@code DatabaseCommand} при условии, что первый байт уже был считан.
     *
     * @return считанная {@code DatabaseCommand}
     * @throws IOException если произошла ошибка ввода-вывода или данные не соответствуют формату RESP
     */
    public DatabaseCommand readCommand() throws IOException {
        final RespObject[] objects = reader.readArray().getObjects();
        if (objects.length < 1) {
            throw new IllegalArgumentException("Command name is not specified");
        }

        final String[] args = Arrays.stream(objects)
            .map(RespObject::asString)
            .toArray(String[]::new);

        return DatabaseCommands.valueOf(args[0]).getCommand(env, args);
    }
}
