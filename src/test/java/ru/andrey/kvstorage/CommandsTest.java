package ru.andrey.kvstorage;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseInitializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializer;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializer;
import ru.andrey.kvstorage.server.logic.Database;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static ru.andrey.kvstorage.server.console.DatabaseCommandResult.DatabaseCommandStatus.FAILED;
import static ru.andrey.kvstorage.server.console.DatabaseCommandResult.DatabaseCommandStatus.SUCCESS;

@RunWith(MockitoJUnitRunner.class)
public class CommandsTest {

    private static final String DB_NAME = "db_1";

    private static final String TABLE_NAME = "tb_1";

    private static final String KEY_NAME = "key";

    private static final String VALUE = "value";

    @Mock
    public Database database;

    @Mock
    public ExecutionEnvironment env;


    @InjectMocks
    public DatabaseServer server = new DatabaseServer(env,
            new DatabaseServerInitializer(new DatabaseInitializer(new TableInitializer(new SegmentInitializer()))));

    public CommandsTest() throws IOException, DatabaseException {
    }

    // ================= update key tests =================

    @Test
    public void test_readKey_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName("table")
                .key("key")
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
    }

    @Test
    public void test_readKey_success() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        when(database.read(TABLE_NAME, KEY_NAME)).thenReturn(VALUE);

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
    }

    @Test
    public void test_readKey_exception() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        var message = "Table already exists";
        doThrow(new DatabaseException(message)).when(database).read(TABLE_NAME, KEY_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertEquals(message, result.getErrorMessage());
    }

    // ================= update key tests =================

    @Test
    public void test_updateKey_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName("table")
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
    }

    @Test
    public void test_updateKey_exception() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        var message = "Table already exists";
        doThrow(new DatabaseException(message)).when(database).write(TABLE_NAME, KEY_NAME, VALUE);

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertEquals(message, result.getErrorMessage());
    }

    @Test
    public void test_updateKey_success() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        doNothing().when(database).write(TABLE_NAME, KEY_NAME, VALUE);

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
    }

    // ================= create table tests =================

    @Test
    public void test_createTable_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
    }

    @Test
    public void test_createTable_success() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        doNothing().when(database).createTableIfNotExists(TABLE_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
    }

    @Test
    public void test_createTable_exception() throws DatabaseException {
        var message = "Table already exists";
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        doThrow(new DatabaseException(message)).when(database).createTableIfNotExists(TABLE_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertEquals(message, result.getErrorMessage());
    }

    @Test
    public void test_executeNext_noCommandName() {
        DatabaseCommandResult databaseCommandResult = server.executeNextCommand((String) null);
        assertEquals(FAILED, databaseCommandResult.getStatus());
    }

    @Test
    public void test_executeNext_noCommandFound() {
        DatabaseCommandResult databaseCommandResult = server.executeNextCommand("fake_command_name");
        assertEquals(FAILED, databaseCommandResult.getStatus());
    }

    @Builder
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    static class Command {
        private String name;
        private String dbName;
        private String tableName;
        private String key;
        private String value;

        @Override
        public String toString() {
            return Stream.of(name, dbName, tableName, key, value)
                    .filter(Objects::nonNull)
                    .collect(Collectors.joining(" "));
        }
    }
}