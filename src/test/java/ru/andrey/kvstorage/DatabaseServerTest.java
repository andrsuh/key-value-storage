package ru.andrey.kvstorage;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.DatabaseServer;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseInitializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializer;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializer;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(JUnit4.class)
public class DatabaseServerTest {

    private final Map<String, String> mapStorage = new ConcurrentHashMap<>();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void checkStorageCorrectness() throws DatabaseException {
        DatabaseServerInitializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        ExecutionEnvironment env = new ExecutionEnvironmentImpl(temporaryFolder.getRoot().toPath());
        DatabaseServer databaseServer = new DatabaseServer(env, initializer);

        String dbName = "test_" + new Random().nextInt(1_000_000);
        String tableName = "table";

        RespObject createDbCommand = CommandsTest.Command.builder()
                .name(DatabaseCommands.CREATE_DATABASE.name())
                .dbName(dbName)
                .build()
                .toRespObject();

        RespObject createTableCommand = CommandsTest.Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(dbName)
                .tableName(tableName)
                .build()
                .toRespObject();

        databaseServer.executeNextCommand(createDbCommand)
                .thenRun(() -> databaseServer.executeNextCommand(createTableCommand));

        Random random = new Random();

        List<String> allowedKeys = Stream.generate(() -> random.nextInt(100_000))
                .map(i -> "test_key_" + i)
                .limit(10_000)
                .collect(Collectors.toList());

        Collections.shuffle(allowedKeys);

        for (int i = 0; i < 300_000; i++) {
            DatabaseCommands commandType = random.nextDouble() > 0.9 ? DatabaseCommands.SET_KEY : DatabaseCommands.GET_KEY;

            String key = allowedKeys.get(random.nextInt(allowedKeys.size()));

            switch (commandType) {
                case SET_KEY: {

                    String value = key + "_" + i;

                    RespObject setKeyCommand = CommandsTest.Command.builder()
                            .name(DatabaseCommands.SET_KEY.name())
                            .dbName(dbName)
                            .tableName(tableName)
                            .key(key)
                            .value(value)
                            .build()
                            .toRespObject();

                    databaseServer.executeNextCommand(setKeyCommand);
                    mapStorage.put(key, value);

                    break;
                }
                case GET_KEY: {
                    if (!mapStorage.containsKey(key))
                        break;

                    RespObject getKeyCommand = CommandsTest.Command.builder()
                            .name(DatabaseCommands.GET_KEY.name())
                            .dbName(dbName)
                            .tableName(tableName)
                            .key(key)
                            .build()
                            .toRespObject();

                    databaseServer.executeNextCommand(getKeyCommand)
                            .thenAccept(result -> {
                                if (result.isSuccess() && result.getResult().isPresent())
                                    Assert.assertArrayEquals("Key : " + key,
                                            mapStorage.get(key).getBytes(StandardCharsets.UTF_8),
                                            result.getResult().get());
                            });
                    break;
                }
            }
        }
    }
}