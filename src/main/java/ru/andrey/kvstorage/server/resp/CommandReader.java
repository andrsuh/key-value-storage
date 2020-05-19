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

    public boolean hasNextCommand() throws IOException {
        return reader.hasArray();
    }

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
