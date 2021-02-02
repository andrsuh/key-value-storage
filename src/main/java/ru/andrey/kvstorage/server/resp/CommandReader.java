package ru.andrey.kvstorage.server.resp;

import lombok.AllArgsConstructor;
import ru.andrey.kvstorage.resp.RespReader;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandArgPositions;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;

import java.io.IOException;
import java.util.List;

@AllArgsConstructor
public class CommandReader {

    private final RespReader reader;
    private final ExecutionEnvironment env;

    public boolean hasNextCommand() throws IOException {
        return reader.hasArray();
    }

    public DatabaseCommand readCommand() throws IOException {
        final List<RespObject> commandArgs = reader.readArray().getObjects();
        if (commandArgs.isEmpty()) {
            throw new IllegalArgumentException("Command name is not specified");
        }

        return DatabaseCommands
                .valueOf(commandArgs.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString())
                .getCommand(env, commandArgs);
    }
}
