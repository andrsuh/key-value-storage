package ru.andrey.kvstorage.server.initialization.impl;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import ru.andrey.kvstorage.server.config.DatabaseConfig;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.initialization.TableInitializationContext;
import ru.andrey.kvstorage.server.logic.Database;
import ru.andrey.kvstorage.server.logic.impl.DatabaseImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class DatabaseInitializerTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private static final MockedStatic<DatabaseImpl> dbImplStaticMock = mockStatic(DatabaseImpl.class);
    private final TableInitializer tableInitializer = mock(TableInitializer.class);
    private final DatabaseInitializer dbInitializer = new DatabaseInitializer(tableInitializer);

    @AfterClass
    public static void down() {
        dbImplStaticMock.close();
    }

    @Test
    public void invalidDbInContext_exceptionIsThrown() {
        try {
            dbInitializer.perform(new InitializationContextImpl(null,
                    new DatabaseInitializationContextImpl("db", temporaryFolder.getRoot().toPath()),
                    null,
                    null));
            fail();
        } catch (DatabaseException e) {
            verifyNoMoreInteractions(tableInitializer);
        }
    }

    @Test
    public void validDbInContextNoTables_tableInitIsNotStarted_dbAddedToEnv() throws DatabaseException, IOException {
        temporaryFolder.newFolder("db");

        ArgumentCaptor<DatabaseInitializationContextImpl> dbContextCaptor = ArgumentCaptor.forClass(DatabaseInitializationContextImpl.class);
        dbImplStaticMock.when(() -> DatabaseImpl.initializeFromContext(dbContextCaptor.capture())).thenReturn(new DbStub("db"));

        //act
        ExecutionEnvironmentImpl executionEnvironment = new ExecutionEnvironmentImpl(new DatabaseConfig(temporaryFolder.getRoot().getPath()));
        DatabaseInitializationContextImpl dbContext = new DatabaseInitializationContextImpl("db", temporaryFolder.getRoot().toPath());
        InitializationContextImpl context = new InitializationContextImpl(executionEnvironment,
                dbContext,
                null,
                null);
        dbInitializer.perform(context);

        assertTrue("Empty db wasn't added to execution context", context.executionEnvironment().getDatabase("db").isPresent());

        verifyNoMoreInteractions(tableInitializer);

        List<DatabaseInitializationContextImpl> createdDb = dbContextCaptor.getAllValues();
        assertThat("Not only one db was created from context", createdDb.size(), is(1));
        assertThat("Created from context db has incorrect name", createdDb.get(0).getDbName(), is("db"));
    }

    @Test
    public void validDbInContext_tablesInitializationStarted_dbAddedToEnv() throws DatabaseException, IOException {
        File testDb = temporaryFolder.newFolder("db");
        Files.createDirectory(testDb.toPath().resolve("table1"));
        Files.createDirectory(testDb.toPath().resolve("table2"));

        ArgumentCaptor<DatabaseInitializationContextImpl> dbContextCaptor = ArgumentCaptor.forClass(DatabaseInitializationContextImpl.class);
        dbImplStaticMock.when(() -> DatabaseImpl.initializeFromContext(dbContextCaptor.capture())).thenReturn(new DbStub("db"));

        ArgumentCaptor<InitializationContextImpl> downstreamContextCaptor = ArgumentCaptor.forClass(InitializationContextImpl.class);
        doNothing().when(tableInitializer).perform(downstreamContextCaptor.capture());

        //act
        ExecutionEnvironmentImpl executionEnvironment = new ExecutionEnvironmentImpl(new DatabaseConfig(temporaryFolder.getRoot().getPath()));
        DatabaseInitializationContextImpl dbContext = new DatabaseInitializationContextImpl("db", temporaryFolder.getRoot().toPath());
        InitializationContextImpl context = new InitializationContextImpl(executionEnvironment,
                dbContext,
                null,
                null);
        dbInitializer.perform(context);

        assertTrue("Db wasn't added to execution context", context.executionEnvironment().getDatabase("db").isPresent());

        List<InitializationContextImpl> capturedTableContexts = downstreamContextCaptor.getAllValues();
        assertThat("Table initialization wasn't performed for all tables", capturedTableContexts.size(), is(2));
        Set<String> tableNames = capturedTableContexts.stream()
                .map(InitializationContextImpl::currentTableContext)
                .map(TableInitializationContext::getTableName)
                .collect(Collectors.toSet());
        assertTrue("Table initialization context hasn't correct table name", tableNames.contains("table1"));
        assertTrue("Table initialization context hasn't correct table name", tableNames.contains("table2"));

        List<DatabaseInitializationContextImpl> createdDb = dbContextCaptor.getAllValues();
        assertThat("Db creation from context wasn't called only once", createdDb.size(), is(1));
        assertThat("Db creation from context wasn't called with correct name", createdDb.get(0).getDbName(), is("db"));
    }

    private static class DbStub implements Database {
        private final String name;

        private DbStub(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void createTableIfNotExists(String tableName) {
        }

        @Override
        public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        }

        @Override
        public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(String tableName, String objectKey) {
        }
    }
}