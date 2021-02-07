package ru.andrey.kvstorage.server.initialization.impl;

import lombok.extern.slf4j.Slf4j;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.InitializationContext;
import ru.andrey.kvstorage.server.initialization.Initializer;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class DatabaseServerInitializer implements Initializer {
    private final Initializer databaseInitializer;

    public DatabaseServerInitializer(Initializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        log.info("Starting initialization process... ");

        ExecutionEnvironment env = context.executionEnvironment();

        try {
            if (!env.getWorkingPath().toFile().exists()) {
                log.info("Creating working directory {}", env.getWorkingPath().toString());
                boolean success = env.getWorkingPath().toFile().mkdirs();
                if (!success)
                    throw new IOException("Directory was not created");
            } else {
                log.info("Using existing working directory {}", env.getWorkingPath().toString());
            }
        } catch (IOException ex) {
            throw new DatabaseException("Cannot create working directory (" + env.getWorkingPath().toString() + ")", ex);
        }

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
            log.info("Initialization process completed");
        } catch (Exception e) { // todo sukhoa handle this. refactor
            throw new DatabaseException(e);
        }
    }
}
