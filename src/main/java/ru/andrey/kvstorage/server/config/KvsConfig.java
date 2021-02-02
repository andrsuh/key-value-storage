package ru.andrey.kvstorage.server.config;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class KvsConfig {
    ServerConfig serverConfig;

    DatabaseConfig dbConfig;
}
