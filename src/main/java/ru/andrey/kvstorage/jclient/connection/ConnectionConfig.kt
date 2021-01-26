package ru.andrey.kvstorage.jclient.connection

data class ConnectionConfig(
        val host: String = "localhost",
        val port: Int = 8080,
        val poolSize: Int = 10
)