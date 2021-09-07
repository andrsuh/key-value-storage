package ru.andrey.kvstorage.server.config;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
@Builder
public class DatabaseServerConfig {
    private final ServerConfig serverConfig;

    private final DatabaseConfig dbConfig;
}
