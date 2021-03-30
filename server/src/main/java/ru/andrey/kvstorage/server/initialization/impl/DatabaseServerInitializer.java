package ru.andrey.kvstorage.server.initialization.impl;

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

public class DatabaseServerInitializer implements Initializer {
    private final DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, нацинает их инициалиализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        System.out.println("Starting initialization process... ");

        ExecutionEnvironment env = context.executionEnvironment();

        try {
            if (!env.getWorkingPath().toFile().exists()) {
                System.out.println("Creating working directory " + env.getWorkingPath().toString());
                boolean success = env.getWorkingPath().toFile().mkdirs();
                if (!success)
                    throw new IOException("Directory was not created");
            } else {
                System.out.println("Using existing working directory " + env.getWorkingPath().toString());
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
        } catch (Exception e) { // todo sukhoa handle this. refactor
            throw new DatabaseException(e);
        }
    }
}
