package ru.andrey.kvstorage.server.initialization;

import org.junit.Test;
import org.mockito.Mockito;
import ru.andrey.kvstorage.server.config.DatabaseConfig;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.logic.Database;

import static org.junit.Assert.*;

public class ExecutionEnvironmentTest {

    @Test
    public void createEnvironment_ReturnValidWorkingPath() {
        ExecutionEnvironment environment = new ExecutionEnvironmentImpl(new DatabaseConfig("db_files"));
        assertEquals("db_files", environment.getWorkingPath().toString());
    }

    @Test
    public void addDatabases_ReturnEqualDatabases() {
        ExecutionEnvironment environment = new ExecutionEnvironmentImpl(new DatabaseConfig("db_files"));
        String dbName1 = "testdb1";
        String dbName2 = "testdb2";

        Database database1 = Mockito.mock(Database.class);
        Database database2 = Mockito.mock(Database.class);

        Mockito.when(database1.getName()).thenReturn(dbName1);
        Mockito.when(database2.getName()).thenReturn(dbName2);

        environment.addDatabase(database1);
        environment.addDatabase(database2);

        String notFoundMessage = "Environment returns empty optional";
        String notEqualMessage = "Database from environment is not equal to original";
        String nullInsteadOfOptional = "Returns null instead of optional";
        String nonEmptyOptional = "Returns non-empty optional";

        assertTrue(notFoundMessage, environment.getDatabase(dbName1).isPresent());
        assertTrue(notFoundMessage, environment.getDatabase(dbName2).isPresent());

        assertEquals(notEqualMessage, database1, environment.getDatabase(dbName1).get());
        assertEquals(notEqualMessage, database2, environment.getDatabase(dbName2).get());

        assertNotNull(nullInsteadOfOptional, environment.getDatabase("notfound"));

        assertTrue(nonEmptyOptional, environment.getDatabase("notfound").isEmpty());
    }
}
