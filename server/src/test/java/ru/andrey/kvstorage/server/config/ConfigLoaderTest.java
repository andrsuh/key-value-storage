package ru.andrey.kvstorage.server.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConfigLoaderTest {

    @Test
    public void configFileNotFound_UseDefaultValues() {
        ConfigLoader configLoader = new ConfigLoader("example.properties");
        DatabaseServerConfig config = configLoader.readConfig();
        assertEquals("Wrong working path", DatabaseConfig.DEFAULT_WORKING_PATH, config.getDbConfig().getWorkingPath());
        assertEquals("Wrong host", ServerConfig.DEFAULT_HOST, config.getServerConfig().getHost());
        assertEquals("Wrong port", ServerConfig.DEFAULT_PORT, config.getServerConfig().getPort());
    }

    @Test
    public void configFileExists_ReadAllProperties() {
        String path = "example_path";
        String host = "127.0.0.2";
        int port = 8082;
        ConfigLoader configLoader = new ConfigLoader("configtest1.properties");
        DatabaseServerConfig config = configLoader.readConfig();
        assertEquals("Wrong working path", path, config.getDbConfig().getWorkingPath());
        assertEquals("Wrong host", host, config.getServerConfig().getHost());
        assertEquals("Wrong port", port, config.getServerConfig().getPort());
    }

    @Test
    public void configFileWithNoPathProperty_ReadPropertiesCorrectly() {
        String path = DatabaseConfig.DEFAULT_WORKING_PATH;
        String host = "127.0.0.2";
        int port = 8082;
        ConfigLoader configLoader = new ConfigLoader("configtest2.properties");
        DatabaseServerConfig config = configLoader.readConfig();
        assertEquals("Wrong working path", path, config.getDbConfig().getWorkingPath());
        assertEquals("Wrong host", host, config.getServerConfig().getHost());
        assertEquals("Wrong port", port, config.getServerConfig().getPort());
    }

    @Test
    public void configFileWithNoPortProperty_ReadPropertiesCorrectly() {
        String path = "example_path";
        String host = "127.0.0.2";
        int port = ServerConfig.DEFAULT_PORT;
        ConfigLoader configLoader = new ConfigLoader("configtest3.properties");
        DatabaseServerConfig config = configLoader.readConfig();
        assertEquals("Wrong working path", path, config.getDbConfig().getWorkingPath());
        assertEquals("Wrong host", host, config.getServerConfig().getHost());
        assertEquals("Wrong port", port, config.getServerConfig().getPort());
    }

    @Test
    public void configFileWithNoHostProperty_ReadPropertiesCorrectly() {
        String path = "example_path";
        String host = ServerConfig.DEFAULT_HOST;
        int port = 8082;
        ConfigLoader configLoader = new ConfigLoader("configtest4.properties");
        DatabaseServerConfig config = configLoader.readConfig();
        assertEquals("Wrong working path", path, config.getDbConfig().getWorkingPath());
        assertEquals("Wrong host", host, config.getServerConfig().getHost());
        assertEquals("Wrong port", port, config.getServerConfig().getPort());
    }
}
