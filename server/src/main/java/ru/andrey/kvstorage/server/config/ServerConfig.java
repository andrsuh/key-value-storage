package ru.andrey.kvstorage.server.config;

import lombok.Value;

@Value
public class ServerConfig {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8080;

    String host;
    int port;
}
