package ru.andrey.kvstorage.server.initialization.impl;

import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.InitializationContext;
import ru.andrey.kvstorage.server.initialization.Initializer;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DatabaseServerInitializer implements Initializer {
    private final Initializer databaseInitializer;

    public DatabaseServerInitializer(Initializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        System.out.println("Starting initialization process... ");

        ExecutionEnvironment env = context.executionEnvironment();

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(env.getWorkingPath(), p -> Files.isDirectory(p)); // todo sukhoa make faster by making parallel
             Stream<Path> directoryStream = StreamSupport.stream(ds.spliterator(), true)) {

            directoryStream.forEach(s -> {
                String databaseName = s.getFileName().toString();

                InitializationContextImpl downstreamContext = InitializationContextImpl.builder()
                        .executionEnvironment(env)
                        .currentDatabaseContext(new DatabaseInitializationContextImpl(databaseName, env.getWorkingPath()))
                        .build();
                try {
                    databaseInitializer.perform(downstreamContext);
                } catch (DatabaseException e) { // todo sukhoa make them throw unchecked exception
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) { // todo sukhoa handle this. refactor
            throw new DatabaseException(e);
        }
    }
}
