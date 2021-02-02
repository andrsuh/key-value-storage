package ru.andrey.kvstorage.jclient.command;


import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespCommandId;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Произвольная строковая команда
 */
public class StringKsvCommand implements KvsCommand {
    private final RespCommandId commandId = new RespCommandId(KvsCommand.idGen.getAndIncrement());
    private final String command;

    public StringKsvCommand(String command) {
        this.command = command;
    }

    @Override
    public RespObject serialize() {
        Stream<RespBulkString> respBulkStringStream = Arrays.stream(command.split(" "))
                .map(str -> new RespBulkString(str.getBytes(StandardCharsets.UTF_8)));

        return new RespArray(Stream.concat(Stream.of(commandId), respBulkStringStream).toArray(RespObject[]::new));
    }

    @Override
    public int getCommandId() {
        return commandId.commandId;
    }
}
