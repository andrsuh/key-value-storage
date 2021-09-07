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

public class CreateTableCommandTest {

    private final ExecutionEnvironment env = Mockito.mock(ExecutionEnvironment.class);

    @Test
    public void ctor_WhenValidArgs_DoNotThrowException() {
        new CreateTableCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.CREATE_TABLE.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void ctor_WhenNotEnoughArgs_ThrowException() {
        assertThrows(Exception.class, () ->
                new CreateTableCommand(
                        env,
                        List.of(new RespCommandId(0),
                                new RespBulkString(DatabaseCommands.CREATE_TABLE.name().getBytes(StandardCharsets.UTF_8)),
                                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8))))
        );
    }

    @Test
    public void execute_WhenDbFound_AddTableToDbAndReturnSuccessResult() throws DatabaseException {
        String tableName = "TEST";

        Mockito.doNothing().when(env).addDatabase(Mockito.any());
        Database db = Mockito.mock(Database.class);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        Mockito.doNothing().when(db).createTableIfNotExists(captor.capture());
        Mockito.when(env.getDatabase(Mockito.any())).thenReturn(Optional.of(db));

        var cmd = new CreateTableCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.CREATE_TABLE.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8))));
        var result = cmd.execute();

        assertTrue("Result is not success", result.isSuccess());
        assertEquals("Invalid name passed to Database.createTableIfNotExists", tableName, captor.getValue());
    }

    @Test
    public void execute_WhenDbNotFound_ReturnErrorResult() {
        Mockito.when(env.getDatabase(Mockito.any())).thenReturn(Optional.empty());

        var cmd = new CreateTableCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.CREATE_TABLE.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8))));
        var result = cmd.execute();

        assertFalse("DB not found, but result is success", result.isSuccess());
    }

    @Test
    public void execute_WhenCreateTableThrowsException_ReturnErrorResult() throws DatabaseException {
        Database db = Mockito.mock(Database.class);
        Mockito.doThrow(new DatabaseException("createTable test exception"))
                .when(db).createTableIfNotExists(Mockito.any());
        Mockito.when(env.getDatabase(Mockito.any())).thenReturn(Optional.of(db));

        var cmd = new CreateTableCommand(env, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.CREATE_TABLE.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8))));
        var result = cmd.execute();

        assertFalse("createTable throws exception but result is success", result.isSuccess());
    }
}