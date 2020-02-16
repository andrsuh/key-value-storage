package ru.andrey.kvstorage;

import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.logic.Database;
import ru.andrey.kvstorage.logic.impl.DatabaseImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Main {
    private static Map<String, Database> databases = new HashMap<>();

    public static void main(String[] args) {

        while (true) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                String[] commandInfo = reader.readLine().split(" ");
                switch (OPS.valueOf(commandInfo[0])) {
                    case CREATE_DATABASE: {
                        Database database = DatabaseImpl.create(commandInfo[1], Path.of(""));
                        databases.put(commandInfo[1], database);
                        break;
                    }
                    case INIT_DATABASE: {
                        Database database = DatabaseImpl.existing(commandInfo[1], Path.of(""));
                        databases.put(commandInfo[1], database);
                        break;
                    }
                    case CREATE_TABLE: { // doesn't work
                        Database database = databases.get(commandInfo[1]); // NPE AIOOB
                        database.createTableIfNotExists(commandInfo[2]); // AIOOB
                        break;
                    }
                    case UPDATE_KEY: {
                        Database database = databases.get(commandInfo[1]); // NPE AIOOB
                        database.write(commandInfo[2], commandInfo[3], commandInfo[4]); // AIOOB
                        break;
                    }
                    case READ_KEY: {
                        Database database = databases.get(commandInfo[1]); // NPE AIOOB
                        String value = database.read(commandInfo[2], commandInfo[3]);// AIOOB
                        System.out.println(value);
                        break;
                    }
                    default:
                        System.out.println("No such command: " + Arrays.toString(commandInfo));
                }

            } catch (IOException | DatabaseException e) {
                e.printStackTrace();
            }
        }
    }

    enum OPS {
        INIT_DATABASE,
        CREATE_DATABASE,
        CREATE_TABLE,
        UPDATE_KEY,
        READ_KEY
    }


    //    public static void main(String[] args) throws IOException, DatabaseException {
////        long start = System.currentTimeMillis();
//        Database db = DatabaseImpl.existing("test_1", Path.of(""));
////        System.out.println("Initialization took: " + (System.currentTimeMillis() - start));
//
////        start = System.currentTimeMillis();
//        System.out.println(db.read("test_1", "1"));
////        System.out.println("Read took: " + (System.currentTimeMillis() - start));
//        System.out.println(db.read("test_1", "2"));
//        System.out.println(db.read("test_1", "3"));
//
//        int n = 0;
//        for (int i = 0; i < 1_000_000; i++) {
//            db.write("test_1", "1", "{" + n++ + "}");
//            db.write("test_1", "2", "{" + n++ + "}");
//            db.write("test_1", "3", "{" + n++ + "}");
//        }
//
//        System.out.println(db.read("test_1", "1"));
//        System.out.println(db.read("test_1", "2"));
//        System.out.println(db.read("test_1", "3"));
//    }

//    public static void main(String[] args) throws IOException, DatabaseException {
//        Database db = DatabaseImpl.create("test_1", Path.of(""));
//        db.createTableIfNotExists("test_1");
//
//        db.write("test_1", "1", "{1}");
//        db.write("test_1", "2", "{2}");
//        db.write("test_1", "3", "{3}");
//
//        System.out.println(db.read("test_1", "1"));
//        System.out.println(db.read("test_1", "2"));
//        System.out.println(db.read("test_1", "3"));
//    }
}
