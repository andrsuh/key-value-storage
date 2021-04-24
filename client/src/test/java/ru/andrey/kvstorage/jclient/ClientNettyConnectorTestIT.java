package ru.andrey.kvstorage.jclient;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import ru.andrey.kvstorage.jclient.client.SimpleKvsClient;
import ru.andrey.kvstorage.jclient.connection.ConnectionConfig;
import ru.andrey.kvstorage.jclient.connection.ConnectionPool;
import ru.andrey.kvstorage.server.DatabaseServer;
import ru.andrey.kvstorage.server.config.DatabaseConfig;
import ru.andrey.kvstorage.server.config.DatabaseServerConfig;
import ru.andrey.kvstorage.server.config.ServerConfig;
import ru.andrey.kvstorage.server.connector.NettyServerConnector;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseInitializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializer;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class ClientNettyConnectorTestIT {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private SimpleKvsClient client;

    private NettyServerConnector connector;

    private static final String DATABASE_NAME = "test";
    private static final String TABLE_NAME = "test_table";

    @Before
    public void setUp() throws DatabaseException, InterruptedException {
        DatabaseServerConfig testConfig = DatabaseServerConfig.builder()
                .dbConfig(new DatabaseConfig(temporaryFolder.getRoot().getAbsolutePath()))
                .serverConfig(new ServerConfig("127.0.0.1", 8080))
                .build();

        DatabaseServerInitializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        ExecutionEnvironment env = new ExecutionEnvironmentImpl(testConfig.getDbConfig());
        DatabaseServer databaseServer = new DatabaseServer(env, initializer);

        connector = new NettyServerConnector(databaseServer, testConfig.getServerConfig());

        ConnectionPool connectionPool = new ConnectionPool(new ConnectionConfig());
        this.client = new SimpleKvsClient(DATABASE_NAME, connectionPool::getClientConnection);

        client.createDatabase(DATABASE_NAME);
        client.executeCommand("CREATE_TABLE " + DATABASE_NAME + " " + TABLE_NAME);
    }

    @After
    public void tearDown() {
        connector.close();
    }

    @Test
    public void test() {
        assertNull(client.get(TABLE_NAME, "key"));
        assertNull(client.set(TABLE_NAME, "key", "oldValue"));
        assertEquals("oldValue", client.set(TABLE_NAME, "key", "newValue"));
        assertNull(client.set(TABLE_NAME, "key1", "value1"));
        assertEquals("newValue", client.get(TABLE_NAME, "key"));
        assertEquals("value1", client.get(TABLE_NAME, "key1"));
        assertEquals("newValue", client.delete(TABLE_NAME, "key"));
        assertNull(client.get(TABLE_NAME, "key"));
    }
}