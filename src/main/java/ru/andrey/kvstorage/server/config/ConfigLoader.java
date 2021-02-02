package ru.andrey.kvstorage.server.config;

import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.Properties;

@RequiredArgsConstructor
public class ConfigLoader {

    private final String name;

    public ConfigLoader() {
        this("server.properties");
    }

    public KvsConfig readConfig() {
        Properties serverProperties = new Properties();
        try {
            serverProperties.load(this.getClass().getClassLoader().getResourceAsStream("server.properties"));
        } catch (IOException ex) {
            System.out.println("Error: server.properties file not found, using default values");
        }

        String workingPath = serverProperties.getProperty("kvs.workingPath", DatabaseConfig.DEFAULT_WORKING_PATH);
        String host = serverProperties.getProperty("kvs.host", ServerConfig.DEFAULT_HOST);
        String portString = serverProperties.getProperty("kvs.port", String.valueOf(ServerConfig.DEFAULT_PORT));
        int port = Integer.parseInt(portString);

        return KvsConfig.builder()
                .dbConfig(new DatabaseConfig(workingPath))
                .serverConfig(new ServerConfig(host, port))
                .build();
    }
}
