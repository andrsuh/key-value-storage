package ru.andrey.kvstorage.server.config;

import lombok.RequiredArgsConstructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
@RequiredArgsConstructor
public class ConfigLoader {

    /**
     * Имя конфикурационного файла, откуда читать
     */
    private final String name;

    public ConfigLoader() {
        this("server.properties");
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного фойла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти
     */
    public DatabaseServerConfig readConfig() {
        Properties serverProperties = new Properties();
        try {
            InputStream stream = Optional
                    .ofNullable(this.getClass().getClassLoader().getResourceAsStream(name))
                    .orElseGet(() -> {
                        try {
                            return new FileInputStream(name);
                        } catch (FileNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                    });
            serverProperties.load(stream);
        } catch (Exception ex) {
            System.out.println("Error: server.properties file not found, using default values");
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
