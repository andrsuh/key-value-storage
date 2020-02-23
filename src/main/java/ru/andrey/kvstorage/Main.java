package ru.andrey.kvstorage;

import ru.andrey.kvstorage.console.DatabaseCommandResult;
import ru.andrey.kvstorage.console.DatabaseCommands;
import ru.andrey.kvstorage.console.ExecutionEnvironment;
import ru.andrey.kvstorage.console.impl.ExecutionEnvironmentImpl;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Main {

    private static final ExecutionEnvironment env = new ExecutionEnvironmentImpl();

    public static void main(String[] args) {

        while (true) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                while (true) {
                    String[] commandInfo = reader.readLine().split(" ");
                    DatabaseCommands commandType = DatabaseCommands.valueOf(commandInfo[0]);

                    DatabaseCommandResult commandResult = commandType.getCommand(env, commandInfo).execute();
                    if (commandResult.getResult() != null) {
                        System.out.println(commandResult.getResult());
                    }
                }
            } catch (Exception e) { // todo sukhoa fix exception stream is closed
                e.printStackTrace();
            }
        }
    }
}
