package ru.andrey.kvstorage.server.initialization.context;

import org.junit.Test;
import org.mockito.Mockito;
import ru.andrey.kvstorage.server.initialization.DatabaseInitializationContext;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseInitializationContextImpl;
import ru.andrey.kvstorage.server.logic.Table;

import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DatabaseInitializationContextTest {

    private final String dbName = "contexttestdb";
    private final Path dbPath = Path.of(dbName);

    @Test
    public void createContext_ReturnValidName() {
        DatabaseInitializationContext context = new DatabaseInitializationContextImpl(dbName, dbPath);
        assertEquals(dbName, context.getDbName());
    }

    @Test
    public void createContext_ReturnValidPath() {
        DatabaseInitializationContext context = new DatabaseInitializationContextImpl(dbName, dbPath);
        assertEquals(dbPath.resolve(dbName), context.getDatabasePath());
    }

    @Test
    public void createContext_ReturnEmptyTableMap() {
        DatabaseInitializationContext context = new DatabaseInitializationContextImpl(dbName, dbPath);
        assertEquals(0, context.getTables().size());
    }

    @Test
    public void addTables_ReturnValidTables() {
        Table table1 = Mockito.mock(Table.class);
        Table table2 = Mockito.mock(Table.class);

        Mockito.when(table1.getName()).thenReturn("contexttesttable1");
        Mockito.when(table2.getName()).thenReturn("contexttesttable2");

        DatabaseInitializationContext context = new DatabaseInitializationContextImpl(dbName, dbPath);
        context.addTable(table1);
        context.addTable(table2);

        assertTrue(context.getTables().containsKey(table1.getName()));
        assertTrue(context.getTables().containsValue(table1));
        assertEquals(table1, context.getTables().get(table1.getName()));

        assertTrue(context.getTables().containsKey(table2.getName()));
        assertTrue(context.getTables().containsValue(table2));
        assertEquals(table2, context.getTables().get(table2.getName()));
    }
}
