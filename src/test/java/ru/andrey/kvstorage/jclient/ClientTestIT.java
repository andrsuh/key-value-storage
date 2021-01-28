package ru.andrey.kvstorage.jclient;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import ru.andrey.kvstorage.DatabaseNettyServer;
import ru.andrey.kvstorage.jclient.client.SimpleKvsClient;
import ru.andrey.kvstorage.jclient.connection.ConnectionConfig;
import ru.andrey.kvstorage.jclient.connection.ConnectionPool;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.Initializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseInitializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializer;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializer;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ClientTestIT {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private SimpleKvsClient client;

    private static final String DATABASE_NAME = "test";
    private static final String TABLE_NAME = "test_table";

    @Before
    public void setUp() throws DatabaseException, InterruptedException {
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));
        new DatabaseNettyServer(new ExecutionEnvironmentImpl(temporaryFolder.getRoot().toPath()), initializer);

        ConnectionPool connectionPool = new ConnectionPool(new ConnectionConfig());
        this.client = new SimpleKvsClient(DATABASE_NAME, connectionPool::getClientConnection);

        client.executeCommand("CREATE_DATABASE " + DATABASE_NAME);
        client.executeCommand("CREATE_TABLE " + DATABASE_NAME + " " + TABLE_NAME);
    }

    @Test
    public void test() {
        assertEquals("null", client.get(TABLE_NAME, "key"));
        assertEquals("null", client.set(TABLE_NAME, "key", "oldValue"));
        assertEquals("oldValue", client.set(TABLE_NAME, "key", "newValue"));
        assertEquals("null", client.set(TABLE_NAME, "key1", "value1"));
        assertEquals("newValue", client.get(TABLE_NAME, "key"));
        assertEquals("value1", client.get(TABLE_NAME, "key1"));
        assertEquals("newValue", client.delete(TABLE_NAME, "key"));
        assertEquals("null", client.get(TABLE_NAME, "key"));
    }


}