package ru.andrey.kvstorage;

import ru.andrey.kvstorage.logic.Table;
import ru.andrey.kvstorage.logic.impl.TableImpl;

import java.io.IOException;
import java.nio.file.Paths;

public class Main {
    public static void main(String[] args) throws IOException, DatabaseException {
        Table table = new TableImpl("test_1", Paths.get(""));


        table.write("1", "{1}");
        table.write("2", "{2}");
        table.write("3", "{3}");

        System.out.println(table.read("1"));
        System.out.println(table.read("2"));
        System.out.println(table.read("3"));
    }
}
