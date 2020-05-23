package ru.andrey.kvstorage.jclient.connection;

import lombok.Value;

@Value
public class ConnectionConfiguration {
    String host;
    int port;
    String databaseName;
}
