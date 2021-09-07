package integration;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.MethodSorters;
import ru.andrey.kvstorage.jclient.client.KvsClient;
import ru.andrey.kvstorage.jclient.client.SimpleKvsClient;
import ru.andrey.kvstorage.jclient.connection.ConnectionConfig;
import ru.andrey.kvstorage.jclient.connection.KvsConnection;
import ru.andrey.kvstorage.jclient.connection.SocketKvsConnection;
import ru.andrey.kvstorage.jclient.exception.DatabaseExecutionException;
import ru.andrey.kvstorage.server.DatabaseServer;
import ru.andrey.kvstorage.server.config.ConfigLoader;
import ru.andrey.kvstorage.server.config.DatabaseServerConfig;
import ru.andrey.kvstorage.server.connector.JavaSocketServerConnector;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseInitializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializer;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IntegrationTest {

    private static final int OPERATIONS_COUNT = 10_000;
    private static final int MAX_VALUE_SIZE = 10_000;

    private static final List<String> tables = List.of("table1", "table2", "table3");
    private static final Map<String, Map<String, String>> dataWritten = new HashMap<>();

    @ClassRule
    public static final TemporaryFolder dbRootFolder = new TemporaryFolder();

    private static JavaSocketServerConnector connector;
    private static KvsClient client;
    private static KvsConnection connection;

    private long idx = 0;

    @BeforeClass
    public static void setUp() {
        tables.forEach(table -> dataWritten.put(table, new HashMap<>()));
    }

    @Test
    public void N1_startServer() throws IOException, DatabaseException {
        dbRootFolder.create();

        File propertiesFile = dbRootFolder.newFile("server.properties");
        Properties properties = new Properties();
        properties.setProperty("kvs.workingPath", dbRootFolder.getRoot().getAbsolutePath());
        properties.setProperty("kvs.host", "127.0.0.1");
        properties.setProperty("kvs.port", "8081");
        properties.store(new FileOutputStream(propertiesFile), null);

        ConfigLoader configLoader = new ConfigLoader(propertiesFile.getAbsolutePath());
        DatabaseServerConfig config = configLoader.readConfig();
        ExecutionEnvironment env = new ExecutionEnvironmentImpl(config.getDbConfig());
        DatabaseServerInitializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));
        DatabaseServer server = DatabaseServer.initialize(env, initializer);

        connector = new JavaSocketServerConnector(server, config.getServerConfig());
        connector.start();
    }

    @Test
    public void N2_createClient() throws Exception {
        createClient();
    }

    @Test
    public void N3_createDatabase() throws Exception {
        try {
            client.createDatabase();
        } catch (Exception e) {
            createClient();
            throw e;
        }
    }

    @Test
    public void N4_createTables() throws Exception {
        try {
            for (String table : tables) {
                client.createTable(table);
            }
        } catch (Exception e) {
            createClient();
            throw e;
        }
    }

    @Test
    public void N5_initializeBeforeRandomIO() throws Exception {
        try {
            for (String table : tables) {
                for (int i = 0; i < 10; i++) {
                    writeNew(table);
                    System.out.println(i + ": DONE");
                }
            }
        } catch (Exception e) {
            createClient();
            throw e;
        }
    }

    @Test
    public void N6_randomIO() throws Exception {
        try {
            for (int i = 0; i < OPERATIONS_COUNT; i++) {
                getRandomOperation().perform();
            }
        } catch (Exception e) {
            createClient();
            throw e;
        }
    }

    @Test
    public void N7_close() throws Exception {
        if (connection != null)
            connection.close();
        connector.close();
    }

    private void createClient() throws Exception {
        if (connection != null)
            connection.close();
        ConnectionConfig config = new ConnectionConfig("127.0.0.1", 8081);
        connection = new SocketKvsConnection(config);
        client = new SimpleKvsClient("db", () -> connection);
    }

    private void readValid() throws DatabaseExecutionException {
        String table = randomTable();
        String key = getRandomKey(table);
        if (key == null)
            return;
        String value = client.get(table, key);
        assertEquals(dataWritten.get(table).get(key), value);
    }

    private void readInvalid() throws DatabaseExecutionException {
        String table = randomTable();
        String key = getRandomKey(table) + "_INVALID";
        assertNull(client.get(table, key));
    }

    private void writeNew() throws DatabaseExecutionException {
        writeNew(randomTable());
    }

    private void writeNew(String table) throws DatabaseExecutionException {
        String key = generateKey();
        String value = generateValue();
        client.set(table, key, value);
        dataWritten.get(table).put(key, value);
    }

    private void writeExisting() throws DatabaseExecutionException {
        String table = randomTable();
        String key = getRandomKey(table);
        if (key == null)
            return;
        String value = generateValue();
        client.set(table, key, value);
        dataWritten.get(table).put(key, value);
    }

    private void deleteExisting() throws DatabaseExecutionException {
        String table = randomTable();
        String key = getRandomKey(table);
        if (key == null)
            return;
        String value = client.delete(table, key);
        assertEquals(dataWritten.get(table).get(key), value);
        dataWritten.get(table).remove(key);
    }

    private void deleteNonExisting() {
        String table = randomTable();
        String key = generateKey() + "_INVALID";
        assertThrows(DatabaseExecutionException.class, () -> client.delete(table, key));
    }

    private String randomTable() {
        return tables.get(new Random().nextInt(tables.size()));
    }

    private String getRandomKey(String table) {
        List<String> keys = List.copyOf(dataWritten.get(table).keySet());
        if (keys.size() == 0)
            return null;
        Random random = new Random();
        return keys.get(random.nextInt(keys.size()));
    }

    private String generateKey() {
        return "somekey" + idx++;
    }

    private String generateValue() {
        Random random = new Random();
        int size = random.nextInt(MAX_VALUE_SIZE);
        byte[] value = new byte[size];
        for (int i = 0; i < size; i++) {
            value[i] = (byte) (random.nextInt('z' - 'A') + 'A');
        }
        return new String(value);
    }

    static class Operation {

        private final ThrowingRunnable action;

        Operation(ThrowingRunnable action) {
            this.action = action;
        }

        public void perform() {
            try {
                this.action.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final Operation READ_VALID = new Operation(this::readValid);
    private final Operation READ_INVALID = new Operation(this::readInvalid);
    private final Operation WRITE_NEW = new Operation(this::writeNew);
    private final Operation WRITE_EXISTING = new Operation(this::writeExisting);
    private final Operation DELETE_EXISTING = new Operation(this::deleteExisting);
    private final Operation DELETE_NON_EXISTING = new Operation(this::deleteNonExisting);

    private Operation getRandomOperation() {
        Random random = new Random();
        switch (random.nextInt(6)) {
            case 0:
                return READ_INVALID;
            case 1:
                return READ_VALID;
            case 2:
                return WRITE_NEW;
            case 3:
                return WRITE_EXISTING;
            case 4:
                return DELETE_EXISTING;
            case 5:
                return DELETE_NON_EXISTING;
            default:
                throw new RuntimeException("Unknown operation index");
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
