package ru.andrey.kvstorage;

import ru.andrey.kvstorage.console.DatabaseCommandResult;
import ru.andrey.kvstorage.console.DatabaseCommands;
import ru.andrey.kvstorage.console.ExecutionEnvironment;
import ru.andrey.kvstorage.console.impl.DatabaseCommandResultImpl;
import ru.andrey.kvstorage.console.impl.ExecutionEnvironmentImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class DatabaseServer {

    private static final ExecutionEnvironment env = new ExecutionEnvironmentImpl();

    public static void main(String[] args) {

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                DatabaseCommandResult commandResult = executeNextCommand(reader.readLine());

                if (commandResult.getResult() != null) {
                    System.out.println(commandResult.getResult());
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Server disconnected due to exceptions while opening input stream", e);
        }
    }

    static DatabaseCommandResult executeNextCommand(String commandText) {
        try {
            String[] commandInfo = commandText.split(" ");

            DatabaseCommands commandType = DatabaseCommands.valueOf(commandInfo[0]);
            return commandType.getCommand(env, commandInfo).execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return new DatabaseCommandResultImpl(e.getMessage(), null);
        }
    }
}
