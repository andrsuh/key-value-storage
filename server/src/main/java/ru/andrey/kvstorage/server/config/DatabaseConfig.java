package ru.andrey.kvstorage.server.config;

import lombok.Builder;
import lombok.Value;

@Value
public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";

    String workingPath;
}
