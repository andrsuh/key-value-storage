package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.logic.Database;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private static final String DEFAULT_DATABASE_FILES_DIRECTORY_NAME = "db_files";
    private static final int DEFAULT_PORT = 4321;

    private final Map<String, Database> dbs = new HashMap<>();
    private final Path workingPath;
    private final int port;

    public ExecutionEnvironmentImpl() {
        Properties serverProperties = new Properties();
        try {
            serverProperties.load(this.getClass().getClassLoader().getResourceAsStream("server.properties"));
        } catch (IOException ex) {
            System.out.println("server.properties file not found, using default values");
        }

        String workingPathString = (String) serverProperties
                .getOrDefault("workingPath", DEFAULT_DATABASE_FILES_DIRECTORY_NAME);
        workingPath = Path.of(workingPathString);

        port = Integer.parseInt(
                (String) serverProperties.getOrDefault("port", DEFAULT_PORT));
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        return Optional.ofNullable(dbs.get(name));
    }

    @Override
    public void addDatabase(Database db) {
        dbs.put(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return workingPath;
    }

    @Override
    public int getPort() {
        return port;
    }
}
