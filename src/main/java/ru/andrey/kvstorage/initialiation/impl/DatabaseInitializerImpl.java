package ru.andrey.kvstorage.initialiation.impl;

import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.initialiation.DatabaseInitializationContext;
import ru.andrey.kvstorage.initialiation.DatabaseInitializer;
import ru.andrey.kvstorage.initialiation.TableInitializer;
import ru.andrey.kvstorage.logic.Table;
import ru.andrey.kvstorage.logic.impl.TableImpl;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// stateful for lectures
public class DatabaseInitializerImpl implements DatabaseInitializer {
    private final TableInitializer tableInitializer;

    public DatabaseInitializerImpl(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    @Override
    public void prepareContext(DatabaseInitializationContext context) throws DatabaseException {
        System.out.println("Creating database: " + context.getDbName());

        if (!Files.exists(context.getDatabasePath())) { // todo sukhoa race condition
            throw new DatabaseException("Database with such name doesn't exist: " + context.getDbName());
        }

        Map<String, Table> tables = new HashMap<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(context.getDatabasePath(), p -> Files.isDirectory(p));
             Stream<Path> directoryStream = StreamSupport.stream(ds.spliterator(), false)) {

            directoryStream
                    .forEach(d -> {
                        String tableName = d.getFileName().toString();
                        Path tableRootPath = context.getDatabasePath();

                        TableInitializationContextImpl tableInitContext = new TableInitializationContextImpl(tableName, tableRootPath);
                        try {
                            tableInitializer.prepareContext(tableInitContext);
                        } catch (DatabaseException e) {
                            throw new RuntimeException(e);
                        }

                        Table table = new TableImpl(tableInitContext);
                        tables.put(table.getName(), table);
                    });
        } catch (IOException e) {
            throw new DatabaseException("Cannot initialize database: " + context.getDbName(), e);
        }

        context.setTables(tables);
    }
}
