package ru.andrey.kvstorage;

import ru.andrey.kvstorage.console.DatabaseCommandResult;
import ru.andrey.kvstorage.console.DatabaseCommands;
import ru.andrey.kvstorage.console.ExecutionEnvironment;
import ru.andrey.kvstorage.console.impl.ExecutionEnvironmentImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class DatabaseServer {

    private final ExecutionEnvironment env;

    public DatabaseServer(ExecutionEnvironment env) {
        this.env = env;
    }

    public static void main(String[] args) {
        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl());

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                DatabaseCommandResult commandResult = databaseServer.executeNextCommand(reader.readLine());

                if (commandResult.isSuccess()) {
                    System.out.println(commandResult.getResult().get());
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Server disconnected due to exceptions while opening input stream", e);
        }
    }

    DatabaseCommandResult executeNextCommand(String commandText) {
        try {
            String[] commandInfo = commandText.split(" ");

            return DatabaseCommands.valueOf(commandInfo[0])
                    .getCommand(env, commandInfo)
                    .execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            String message = e.getMessage() != null
                    ? e.getMessage()
                    : Arrays.toString(e.getStackTrace());
            return DatabaseCommandResult.error(message);
        }
    }
}
