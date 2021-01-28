package ru.andrey.kvstorage;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import ru.andrey.kvstorage.server.DatabaseServer;
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
import static org.junit.Assert.assertTrue;
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

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public DatabaseServer server;

    @Before
    public void setUp() throws IOException, DatabaseException {
        env = mock(ExecutionEnvironment.class);
        when(env.getWorkingPath()).thenReturn(temporaryFolder.getRoot().toPath());

        server = new DatabaseServer(env,
                new DatabaseServerInitializer(new DatabaseInitializer(new TableInitializer(new SegmentInitializer()))));
    }

    @After
    public void closeSocket() throws Exception {
        server.close();
    }

    // ================= read key tests =================

    @Test
    public void test_getKey_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.GET_KEY.name())
                .dbName(DB_NAME)
                .tableName("table")
                .key("key")
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
    }

    @Test
    public void test_getKey_success() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        when(database.read(eq(TABLE_NAME), eq(KEY_NAME))).thenReturn(Optional.of(VALUE));

        Command command = Command.builder()
                .name(DatabaseCommands.GET_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult commandResult = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, commandResult.getStatus());
        //noinspection OptionalGetWithoutIsPresent
        assertEquals(VALUE, commandResult.getResult().get());
    }

    @Test
    public void test_getKey_noSuchKey() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        when(database.read(eq(TABLE_NAME), eq(KEY_NAME))).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.GET_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult commandResult = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, commandResult.getStatus());
        assertTrue(commandResult.getResult().isEmpty());
    }

    @Test
    public void test_getKey_exception() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        var message = "Table already exists";
        doThrow(new DatabaseException(message)).when(database).read(TABLE_NAME, KEY_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.GET_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertEquals(message, result.getErrorMessage());
    }

    // ================= set key tests =================

    @Test
    public void test_setKey_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.SET_KEY.name())
                .dbName(DB_NAME)
                .tableName("table")
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
    }

    @Test
    public void test_setKey_noPrevValue_success() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        when(database.read(eq(TABLE_NAME), eq(KEY_NAME))).thenReturn(Optional.empty());
        doNothing().when(database).write(TABLE_NAME, KEY_NAME, VALUE);

        Command command = Command.builder()
                .name(DatabaseCommands.SET_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
        assertTrue( result.getResult().isEmpty());
    }

    @Test
    public void test_setKey_hasPrevValue_success() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        when(database.read(eq(TABLE_NAME), eq(KEY_NAME))).thenReturn(Optional.of("prev"));
        doNothing().when(database).write(TABLE_NAME, KEY_NAME, VALUE);

        Command command = Command.builder()
                .name(DatabaseCommands.SET_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
        //noinspection OptionalGetWithoutIsPresent
        assertEquals("prev", result.getResult().get());
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
            return Stream.of("0", name, dbName, tableName, key, value)
                    .filter(Objects::nonNull)
                    .collect(Collectors.joining(" "));
        }
    }
}