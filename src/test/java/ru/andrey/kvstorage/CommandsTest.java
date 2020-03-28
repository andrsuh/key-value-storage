package ru.andrey.kvstorage;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import ru.andrey.kvstorage.console.DatabaseCommandResult;
import ru.andrey.kvstorage.console.DatabaseCommands;
import ru.andrey.kvstorage.console.ExecutionEnvironment;
import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.logic.Database;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.when;
import static ru.andrey.kvstorage.console.DatabaseCommandResult.DatabaseCommandStatus.FAILED;
import static ru.andrey.kvstorage.console.DatabaseCommandResult.DatabaseCommandStatus.SUCCESS;

@RunWith(MockitoJUnitRunner.class)
public class CommandsTest {
    @Mock
    public Database database;

    @Mock
    public ExecutionEnvironment env;

    @InjectMocks
    public DatabaseServer server = new DatabaseServer(env);

    @Test
    public void test_read_noSuchDb() {
        String dbName = "db_1";
        when(env.getDatabase(dbName)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(dbName)
                .tableName("table")
                .key("key")
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        Assert.assertEquals(FAILED, result.getStatus());
    }

    @Test
    public void test_read_success() throws DatabaseException {
        String dbName = "db_1";
        String tableName = "tb_1";
        String keyName = "key";
        String value = "value";

        when(env.getDatabase(dbName)).thenReturn(Optional.of(database));
        when(env.currentDatabase()).thenReturn(Optional.empty());
        when(database.read(tableName, keyName)).thenReturn(value);

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(dbName)
                .tableName(tableName)
                .key(keyName)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        Assert.assertEquals(SUCCESS, result.getStatus());
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