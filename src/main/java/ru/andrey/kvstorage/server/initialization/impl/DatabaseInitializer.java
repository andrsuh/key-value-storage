package ru.andrey.kvstorage.server.initialization.impl;

import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.TableIndex;
import ru.andrey.kvstorage.server.initialization.DatabaseInitializationContext;
import ru.andrey.kvstorage.server.initialization.InitializationContext;
import ru.andrey.kvstorage.server.initialization.Initializer;
import ru.andrey.kvstorage.server.initialization.TableInitializationContext;
import ru.andrey.kvstorage.server.logic.impl.DatabaseImpl;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// stateless for lectures
public class DatabaseInitializer implements Initializer {
    private final Initializer tableInitializer;

    public DatabaseInitializer(Initializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        DatabaseInitializationContext databaseContext = initialContext.currentDbContext();

        System.out.println("Creating database: " + databaseContext.getDbName());

        if (!Files.exists(databaseContext.getDatabasePath())) {
            throw new DatabaseException("Database with such name doesn't exist: " + databaseContext.getDbName());
        }

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(databaseContext.getDatabasePath(), p -> Files.isDirectory(p));
             Stream<Path> directoryStream = StreamSupport.stream(ds.spliterator(), false)) {

            directoryStream
                    .forEach(d -> {
                        String tableName = d.getFileName().toString();
                        Path tableRootPath = databaseContext.getDatabasePath();

                        TableInitializationContext tableInitContext = new TableInitializationContextImpl(tableName, tableRootPath, new TableIndex());

                        InitializationContext downstreamContext = InitializationContextImpl.builder()
                                .executionEnvironment(initialContext.executionEnvironment())
                                .currentDatabaseContext(initialContext.currentDbContext())
                                .currentTableContext(tableInitContext)
                                .build();
                        try {
                            tableInitializer.perform(downstreamContext);
                        } catch (DatabaseException e) {
                            throw new RuntimeException(e);
                        }

                        initialContext.executionEnvironment()
                                .addDatabase(DatabaseImpl.initializeFromContext(initialContext.currentDbContext()));
                    });
        } catch (IOException e) {
            throw new DatabaseException("Cannot initialize database: " + databaseContext.getDbName(), e);
        }
    }
}
