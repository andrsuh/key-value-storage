package ru.andrey.kvstorage;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ru.andrey.kvstorage.server.DatabaseServer;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.Initializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseInitializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializer;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializer;

import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

@RunWith(JUnit4.class)
public class DatabaseServerTest {
    private Map<String, String> mapStorage = new ConcurrentHashMap<>();

    @Test
    public void checkStorageCorrectness() throws DatabaseException {
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer, mock(ServerSocket.class));

        String dbName = "test_" + new Random().nextInt(1_000_000);
        String tableName = "table";
        String[] initCommands = {
                "0 CREATE_DATABASE " + dbName,
                "0 CREATE_TABLE " + dbName + " " + tableName
        };

        System.out.println(Arrays.toString(initCommands));

        Arrays.stream(initCommands)
                .forEach(databaseServer::executeNextCommand);

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
                    databaseServer.executeNextCommand(
                            "0 SET_KEY " + dbName + " " + tableName + " " + key + " " + value);
                    mapStorage.put(key, value);

                    break;
                }
                case GET_KEY: {
                    if (!mapStorage.containsKey(key))
                        break;

                    DatabaseCommandResult commandResult = databaseServer.executeNextCommand(
                            "0 GET_KEY " + dbName + " " + tableName + " " + key);

                    if (commandResult.isSuccess()) {
                        //noinspection OptionalGetWithoutIsPresent
                        Assert.assertEquals("Key : " + key, mapStorage.get(key), commandResult.getResult().get());
                    }
                    break;
                }
            }
        }
    }
}