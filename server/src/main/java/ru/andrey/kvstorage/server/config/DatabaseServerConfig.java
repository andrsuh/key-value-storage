package ru.andrey.kvstorage.server.config;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DatabaseServerConfig {
    ServerConfig serverConfig;

    DatabaseConfig dbConfig;
}
