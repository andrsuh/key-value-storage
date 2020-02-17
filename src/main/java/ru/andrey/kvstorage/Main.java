package ru.andrey.kvstorage;

import ru.andrey.kvstorage.console.DatabaseCommandResult;
import ru.andrey.kvstorage.console.DbCommandType;
import ru.andrey.kvstorage.console.ExecutionEnvironment;
import ru.andrey.kvstorage.console.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.initialiation.DatabaseInitializer;
import ru.andrey.kvstorage.initialiation.impl.DatabaseInitializerImpl;
import ru.andrey.kvstorage.initialiation.impl.SegmentInitializerImpl;
import ru.andrey.kvstorage.initialiation.impl.TableInitializerImpl;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Main {

    private static final ExecutionEnvironment env = new ExecutionEnvironmentImpl();

    public static void main(String[] args) {

        DatabaseInitializer initializer = new DatabaseInitializerImpl(new TableInitializerImpl(new SegmentInitializerImpl()));

        while (true) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                while (true) {
                    String[] commandInfo = reader.readLine().split(" ");
                    DbCommandType commandType = DbCommandType.valueOf(commandInfo[0]);

                    DatabaseCommandResult commandResult = commandType.getCommand(env, commandInfo).execute();
                    if (commandResult.getResult() != null) {
                        System.out.println(commandResult.getResult());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
//                    switch (DbCommandType.valueOf(commandInfo[0])) {
//                        case CREATE_DATABASE: {
//                            Database database = DatabaseImpl.create(commandInfo[1], Path.of(""));
//                            databases.put(commandInfo[1], database);
//                            break;
//                        }
//                        case INIT_DATABASE: {
//                        Database database = DatabaseImpl.existing(commandInfo[1], Path.of(""));
//                        databases.put(commandInfo[1], database);
//                            DatabaseInitializationContextImpl context = new DatabaseInitializationContextImpl(commandInfo[1], Path.of(""));
//                            initializer.prepareContext(context);
//                            databases.put(commandInfo[1], new DatabaseImpl(context));
//                            break;
//                        }
//                        case CREATE_TABLE: { // doesn't work
//                            Database database = databases.get(commandInfo[1]); // NPE AIOOB
//                            database.createTableIfNotExists(commandInfo[2]); // AIOOB
//                            break;
//                        }
//                        case UPDATE_KEY: {
//                            Database database = databases.get(commandInfo[1]); // NPE AIOOB
//                            database.write(commandInfo[2], commandInfo[3], commandInfo[4]); // AIOOB
//                            break;
//                        }
//                        case READ_KEY: {
//                            Database database = databases.get(commandInfo[1]); // NPE AIOOB
//                            String value = database.read(commandInfo[2], commandInfo[3]);// AIOOB
//                            System.out.println(value);
//                            break;
//                        }
//                        default:
//                            System.out.println("No such command: " + Arrays.toString(commandInfo));
//                    }

