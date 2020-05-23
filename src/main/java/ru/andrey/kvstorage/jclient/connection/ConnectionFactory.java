package ru.andrey.kvstorage.jclient.connection;

@FunctionalInterface
public interface ConnectionFactory {

    KvsConnection buildConnection(ConnectionConfiguration configuration);
}
