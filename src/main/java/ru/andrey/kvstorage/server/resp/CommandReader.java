package ru.andrey.kvstorage.server.resp;

import lombok.AllArgsConstructor;
import ru.andrey.kvstorage.resp.RespReader;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
public class CommandReader {

    private final RespReader reader;
    private final ExecutionEnvironment env;

    public boolean hasNextCommand() throws IOException {
        return reader.hasArray();
    }

    public DatabaseCommand readCommand() throws IOException {
        final List<RespObject> objects = reader.readArray().getObjects();
        if (objects.isEmpty()) {
            throw new IllegalArgumentException("Command name is not specified");
        }

        final String[] args = objects.stream()
                .map(RespObject::asString)
                .toArray(String[]::new);

        List<String> commandArgs = Arrays.stream(args).skip(1).collect(Collectors.toList());
        return DatabaseCommands.valueOf(commandArgs.get(0)).getCommand(env, commandArgs);
    }
}
