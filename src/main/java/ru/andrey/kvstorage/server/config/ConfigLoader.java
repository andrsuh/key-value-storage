package ru.andrey.kvstorage.server.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Properties;

@RequiredArgsConstructor
@Slf4j
public class ConfigLoader {

    private final String name;

    public ConfigLoader() {
        this("server.properties");
    }

    public DatabaseServerConfig readConfig() {
        Properties serverProperties = new Properties();
        try {
            serverProperties.load(this.getClass().getClassLoader().getResourceAsStream("server.properties"));
        } catch (IOException ex) {
            log.warn("Error: server.properties file not found, using default values");
        }

        String workingPath = serverProperties.getProperty("kvs.workingPath", DatabaseConfig.DEFAULT_WORKING_PATH);
        String host = serverProperties.getProperty("kvs.host", ServerConfig.DEFAULT_HOST);
        String portString = serverProperties.getProperty("kvs.port", String.valueOf(ServerConfig.DEFAULT_PORT));
        int port = Integer.parseInt(portString);

        return DatabaseServerConfig.builder()
                .dbConfig(new DatabaseConfig(workingPath))
                .serverConfig(new ServerConfig(host, port))
                .build();
    }
}
