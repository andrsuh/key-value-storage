package ru.andrey.kvstorage.server.console.impl;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
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

import static org.junit.Assert.*;

public class SetKeyCommandTest {

    private final ExecutionEnvironment env = Mockito.mock(ExecutionEnvironment.class);

    @Test
    public void ctor_WhenValidArgs_DoNotThrowException() {
        new SetKeyCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.SET_KEY.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("key".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("value".getBytes(StandardCharsets.UTF_8)))
        );
    }

    @Test
    public void ctor_WhenNotEnoughArgs_ThrowException() {
        assertThrows(Exception.class, () ->
                new SetKeyCommand(env, List.of(
                        new RespCommandId(0),
                        new RespBulkString(DatabaseCommands.SET_KEY.name().getBytes(StandardCharsets.UTF_8)),
                        new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                        new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                        new RespBulkString("key".getBytes(StandardCharsets.UTF_8))))
        );
    }

    @Test
    public void execute_ChangeValueAndReturnSuccessResult() throws DatabaseException {
        String tableName = "TEST";
        String key = "key";
        String value = "value";

        Database db = Mockito.mock(Database.class);
        Mockito.when(db.read(Mockito.any(), Mockito.any())).thenReturn(Optional.of(new byte[] {1, 2, 3}));
        ArgumentCaptor<String> tableNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
        Mockito.doNothing().when(db).write(tableNameCaptor.capture(), keyCaptor.capture(), valueCaptor.capture());
        Mockito.when(env.getDatabase(Mockito.any())).thenReturn(Optional.of(db));

        var cmd = new SetKeyCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.SET_KEY.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(key.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(value.getBytes(StandardCharsets.UTF_8)))
        );
        var result = cmd.execute();

        assertTrue("Result is not success", result.isSuccess());
        assertEquals("Invalid table name from which key was written", tableName, tableNameCaptor.getValue());
        assertEquals("Invalid key that was written", key, keyCaptor.getValue());
        assertEquals(
                "Invalid value that was written",
                value,
                new String(valueCaptor.getValue(), StandardCharsets.UTF_8)
        );
    }

    @Test
    public void execute_WhenDbNotFound_ReturnErrorResult() {
        String tableName = "TEST";
        String key = "key";
        String value = "value";

        Mockito.when(env.getDatabase(Mockito.any())).thenReturn(Optional.empty());

        var cmd = new SetKeyCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.SET_KEY.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(key.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(value.getBytes(StandardCharsets.UTF_8)))
        );
        var result = cmd.execute();

        assertFalse("DB not found but result is success", result.isSuccess());
    }

    @Test
    public void execute_WhenSetKeyThrowsException_ReturnErrorResult() throws DatabaseException {
        String tableName = "TEST";
        String key = "key";
        String value = "value";

        Database db = Mockito.mock(Database.class);
        Mockito.doThrow(new DatabaseException("Set key test exception")).when(db).delete(Mockito.any(), Mockito.any());
        Mockito.when(env.getDatabase(Mockito.any())).thenReturn(Optional.empty());

        var cmd = new SetKeyCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.SET_KEY.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(key.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(value.getBytes(StandardCharsets.UTF_8)))
        );
        var result = cmd.execute();

        assertFalse("Set key operation throws exception but result is success", result.isSuccess());
    }
}
