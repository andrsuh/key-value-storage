package ru.andrey.kvstorage.jclient;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import ru.andrey.kvstorage.jclient.client.SimpleKvsClient;
import ru.andrey.kvstorage.jclient.connection.ConnectionConfig;
import ru.andrey.kvstorage.jclient.connection.SocketKvsConnection;
import ru.andrey.kvstorage.server.DatabaseServer;
import ru.andrey.kvstorage.server.connector.NettyServerConnector;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseInitializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializer;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class ClientJavaSocketConnectorTestIT {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private SimpleKvsClient client;

    private static final String DATABASE_NAME = "test";
    private static final String TABLE_NAME = "test_table";

    @Before
    public void setUp() throws DatabaseException, InterruptedException {
        DatabaseServerInitializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(temporaryFolder.getRoot().toPath()), initializer);

        new NettyServerConnector(databaseServer);

        this.client = new SimpleKvsClient(DATABASE_NAME, () -> new SocketKvsConnection(new ConnectionConfig()));
        client.executeCommand("CREATE_DATABASE " + DATABASE_NAME);
        client.executeCommand("CREATE_TABLE " + DATABASE_NAME + " " + TABLE_NAME);
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