package ru.andrey.kvstorage.jclient.connection;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Класс содержит информацию, что слушает сервер, по какому адресу с ним взаимодействовать.
 * (По идее они должны совпадать с тем, какие мы используем в server.properties)
 */
@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class ConnectionConfig {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8080;
    public static final int DEFAULT_POOL_SIZE = 10;

    private final String host;
    private final int port;
    private final int poolSize;

    public ConnectionConfig(String host, int port) {
        this.host = host;
        this.port = port;
        this.poolSize = DEFAULT_POOL_SIZE;
    }

    public ConnectionConfig() {
        this.host = DEFAULT_HOST;
        this.port = DEFAULT_PORT;
        this.poolSize = DEFAULT_POOL_SIZE;
    }
}