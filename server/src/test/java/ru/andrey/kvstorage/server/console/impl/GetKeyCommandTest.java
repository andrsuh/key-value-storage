package ru.andrey.kvstorage.server.console.impl;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespCommandId;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.logic.Database;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GetKeyCommandTest {

    private final ExecutionEnvironment env = Mockito.mock(ExecutionEnvironment.class);

    @Test
    public void ctor_WhenValidArgs_DoNotThrowException() {
        new GetKeyCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.GET_KEY.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("key".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void ctor_WhenNotEnoughArgs_ThrowException() {
        assertThrows(Exception.class, () ->
                new GetKeyCommand(env, List.of(
                        new RespCommandId(0),
                        new RespBulkString(DatabaseCommands.GET_KEY.name().getBytes(StandardCharsets.UTF_8)),
                        new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                        new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8))))
        );
    }

    @Test
    public void execute_WhenKeyExists_GetValueAndReturnSuccessResult() throws DatabaseException {
        String tableName = "TEST";
        String key = "key";
        String value = "123";

        Database db = Mockito.mock(Database.class);
        Mockito.when(db.read(Mockito.any(), Mockito.any())).thenReturn(Optional.of(value.getBytes(StandardCharsets.UTF_8)));
        Mockito.when(env.getDatabase(Mockito.any())).thenReturn(Optional.of(db));

        var cmd = new GetKeyCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.GET_KEY.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(key.getBytes(StandardCharsets.UTF_8))));
        var result = cmd.execute();

        assertTrue("Result is not success", result.isSuccess());
        Assert.assertEquals("Command payload not equals value from DB", value, result.getPayLoad());
    }
}