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
import ru.andrey.kvstorage.server.logic.DatabaseFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.*;

public class CreateDatabaseCommandTest {

    private final ExecutionEnvironment env = Mockito.mock(ExecutionEnvironment.class);
    private final DatabaseFactory dbFactory = Mockito.mock(DatabaseFactory.class);

    @Test
    public void ctor_WhenValidArgs_DoNotThrowException() {
        new CreateDatabaseCommand(env, dbFactory, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.CREATE_DATABASE.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void ctor_WhenNotEnoughArgs_ThrowException() {
        assertThrows(Exception.class, () ->
                new CreateDatabaseCommand(
                        env,
                        dbFactory,
                        List.of(new RespCommandId(0),
                                new RespBulkString(DatabaseCommands.CREATE_DATABASE.name()
                                        .getBytes(StandardCharsets.UTF_8))))
        );
    }

    @Test
    public void execute_WhenNoErrorsInDbFactoryAndExecEnv_CreateTableAndReturnSuccessResult() throws DatabaseException {
        ArgumentCaptor<Database> dbCaptor = ArgumentCaptor.forClass(Database.class);
        var dbMock = Mockito.mock(Database.class);
        Mockito.doNothing().when(env).addDatabase(dbCaptor.capture());
        Mockito.when(dbFactory.createNonExistent(Mockito.any(), Mockito.any())).thenReturn(dbMock);

        var cmd = new CreateDatabaseCommand(env, dbFactory, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.CREATE_DATABASE.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8))));
        var result = cmd.execute();

        assertTrue("Result is not success", result.isSuccess());
        assertEquals(dbMock, dbCaptor.getValue());
    }

    @Test
    public void execute_WhenDbFactoryError_ReturnErrorResult() throws DatabaseException {
        Mockito.doNothing().when(env).addDatabase(Mockito.any());
        Mockito.when(dbFactory.createNonExistent(Mockito.any(), Mockito.any()))
                .thenThrow(new DatabaseException("DbFactory test exception"));

        var cmd = new CreateDatabaseCommand(env, dbFactory, List.of(
                new RespCommandId(0),
                new RespBulkString(DatabaseCommands.CREATE_DATABASE.name().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8))));
        var result = cmd.execute();

        assertFalse("DB factory throws exception but result is success", result.isSuccess());
    }
}