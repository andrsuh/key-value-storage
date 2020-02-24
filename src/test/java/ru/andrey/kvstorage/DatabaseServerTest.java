package ru.andrey.kvstorage;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ru.andrey.kvstorage.console.DatabaseCommandResult;
import ru.andrey.kvstorage.console.DatabaseCommands;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(JUnit4.class)
public class DatabaseServerTest {
    private Map<String, String> mapStorage = new ConcurrentHashMap<>();

    @Test
    public void checkStorageCorrectness() {
        String dbName = "test_" + new Random().nextInt(1_000_000);
        String tableName = "table";
        String[] initCommands = {
                "CREATE_DATABASE " + dbName,
                "CREATE_TABLE " + dbName + " " + tableName
        };

        System.out.println(Arrays.toString(initCommands));

        Arrays.stream(initCommands)
                .forEach(DatabaseServer::executeNextCommand);

        Random random = new Random();

        List<String> allowedKeys = Stream.generate(() -> random.nextInt(100_000))
                .map(i -> "test_key_" + i)
                .limit(10_000)
                .collect(Collectors.toList());

        Collections.shuffle(allowedKeys);

        for (int i = 0; i < 100_000; i++) {
            DatabaseCommands commandType = random.nextDouble() > 0.8 ? DatabaseCommands.UPDATE_KEY : DatabaseCommands.READ_KEY;

            String key = allowedKeys.get(random.nextInt(allowedKeys.size()));

            switch (commandType) {
                case UPDATE_KEY: {

                    String value = key + "_" + i;
                    DatabaseServer.executeNextCommand(
                            "UPDATE_KEY " + dbName + " " + tableName + " " + key + " " + value);
                    mapStorage.put(key, value);

                    break;
                }
                case READ_KEY: {
                    if (!mapStorage.containsKey(key))
                        break;

                    DatabaseCommandResult commandResult = DatabaseServer.executeNextCommand(
                            "READ_KEY " + dbName + " " + tableName + " " + key);

                    if (commandResult != null) {
                        Assert.assertEquals("Key : " + key, mapStorage.get(key), commandResult.getResult());
                    }
                    break;
                }
            }
        }
    }
}