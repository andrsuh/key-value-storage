package ru.andrey.kvstorage.server.console.impl;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespCommandId;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.config.DatabaseConfig;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class DatabaseCommandsTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private ExecutionEnvironment environment;
    private List<RespObject> args;

    @Before
    public void setUp() {
        environment = new ExecutionEnvironmentImpl(
                new DatabaseConfig(temporaryFolder.getRoot().getPath())
        );
        args = List.of(
                new RespCommandId(1),
                new RespBulkString("COMMAND_NAME".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("database".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("table".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("key".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("value".getBytes(StandardCharsets.UTF_8))
        );
    }

    @Test
    public void getCommand_ReturnCreateDatabaseCommand() {
        assertThat(
                "CREATE_DATABASE must return CreateDatabaseCommand",
                DatabaseCommands.CREATE_DATABASE.getCommand(environment, args.subList(0, 3)),
                instanceOf(CreateDatabaseCommand.class)
        );
    }

    @Test
    public void getCommand_ReturnCreateTableCommand() {
        assertThat(
                "CREATE_TABLE must return CreateTableCommand",
                DatabaseCommands.CREATE_TABLE.getCommand(environment, args.subList(0, 4)),
                instanceOf(CreateTableCommand.class)
        );
    }

    @Test
    public void getCommand_ReturnGetKeyCommand() {
        assertThat(
                "GET_KEY must return GetKeyCommand",
                DatabaseCommands.GET_KEY.getCommand(environment, args.subList(0, 5)),
                instanceOf(GetKeyCommand.class)
        );
    }

    @Test
    public void getCommand_ReturnDeleteKeyCommand() {
        assertThat(
                "DELETE_KEY must return DeleteKeyCommand",
                DatabaseCommands.DELETE_KEY.getCommand(environment, args.subList(0, 5)),
                instanceOf(DeleteKeyCommand.class)
        );
    }

    @Test
    public void getCommand_ReturnSetKeyCommand() {
        assertThat(
                "SET_KEY must return SetKeyCommand",
                DatabaseCommands.SET_KEY.getCommand(environment, args.subList(0, 6)),
                instanceOf(SetKeyCommand.class)
        );
    }
}
