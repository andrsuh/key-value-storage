package ru.andrey.kvstorage.server.config;

public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";

    String workingPath;

    public String getWorkingPath() {
        return workingPath;
    }

    public DatabaseConfig(String workingPath) {
        this.workingPath = workingPath;
    }
}
