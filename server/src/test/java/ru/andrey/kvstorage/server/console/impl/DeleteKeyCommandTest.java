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

public class DeleteKeyCommandTest {

    private final ExecutionEnvironment env = Mockito.mock(ExecutionEnvironment.class);

    @Test
    public void ctor_WhenValidArgs_DoNotThrowException() {
        new DeleteKeyCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.DELETE_KEY.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("key".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void ctor_WhenNotEnoughArgs_ThrowException() {
        assertThrows(Exception.class, () ->
                new DeleteKeyCommand(env, List.of(
                        new RespCommandId(0),
                        new RespBulkString(DatabaseCommands.DELETE_KEY.name().getBytes(StandardCharsets.UTF_8)),
                        new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                        new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)))
                )
        );
    }

    @Test
    public void execute_WhenKeyExists_DeleteKeyAndReturnSuccessResult() throws DatabaseException {
        String tableName = "TEST";
        String key = "key";

        Database db = Mockito.mock(Database.class);
        Mockito.when(db.read(Mockito.any(), Mockito.any())).thenReturn(Optional.of(new byte[] {1, 2, 3}));
        ArgumentCaptor<String> tableNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.doNothing().when(db).delete(tableNameCaptor.capture(), keyCaptor.capture());
        Mockito.when(env.getDatabase(Mockito.any())).thenReturn(Optional.of(db));

        var cmd = new DeleteKeyCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.DELETE_KEY.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(key.getBytes(StandardCharsets.UTF_8))));
        var result = cmd.execute();

        assertTrue("Result is not success", result.isSuccess());
        assertEquals("Invalid table name from which key was deleted", tableName, tableNameCaptor.getValue());
        assertEquals("Invalid key that was deleted", key, keyCaptor.getValue());
    }

    @Test
    public void execute_WhenDbNotFound_ReturnErrorResult() {
        String tableName = "TEST";
        String key = "key";

        Mockito.when(env.getDatabase(Mockito.any())).thenReturn(Optional.empty());

        var cmd = new DeleteKeyCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.DELETE_KEY.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(key.getBytes(StandardCharsets.UTF_8))));
        var result = cmd.execute();

        assertFalse("DB not found but result is success", result.isSuccess());
    }

    @Test
    public void execute_WhenDeleteThrowsException_ReturnErrorResult() throws DatabaseException {
        String tableName = "TEST";
        String key = "key";

        Database db = Mockito.mock(Database.class);
        Mockito.doThrow(new DatabaseException("Delete key test exception")).when(db).delete(Mockito.any(), Mockito.any());
        Mockito.when(env.getDatabase(Mockito.any())).thenReturn(Optional.empty());

        var cmd = new DeleteKeyCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.DELETE_KEY.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(key.getBytes(StandardCharsets.UTF_8))));
        var result = cmd.execute();

        assertFalse("Delete key operation throws exception but result is success", result.isSuccess());
    }
}