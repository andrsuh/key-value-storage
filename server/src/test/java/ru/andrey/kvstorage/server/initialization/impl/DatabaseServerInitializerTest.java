package ru.andrey.kvstorage.server.initialization.impl;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import ru.andrey.kvstorage.server.config.DatabaseConfig;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.initialization.DatabaseInitializationContext;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class DatabaseServerInitializerTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final DatabaseInitializer databaseInitializer = mock(DatabaseInitializer.class);
    private final DatabaseServerInitializer dbServerInitializer = new DatabaseServerInitializer(databaseInitializer);

    @Test
    public void nothingInWorkingPath_createdDir() throws DatabaseException {
        Path workingPath = temporaryFolder.getRoot().toPath().resolve("db_files");
        dbServerInitializer.perform(new InitializationContextImpl(
                new ExecutionEnvironmentImpl(new DatabaseConfig(workingPath.toString())),
                null,
                null,
                null
        ));

        verifyNoMoreInteractions(databaseInitializer);
        assertTrue("Working path wasn't created", workingPath.toFile().exists());
    }

    @Test
    public void dbExistNoTables_notStartedInitialization_envUpdated() throws DatabaseException {
        ArgumentCaptor<InitializationContextImpl> downstreamContextCaptor = ArgumentCaptor.forClass(InitializationContextImpl.class);
        doNothing().when(databaseInitializer).perform(downstreamContextCaptor.capture());

        //act
        ExecutionEnvironmentImpl executionEnvironment = new ExecutionEnvironmentImpl(new DatabaseConfig(temporaryFolder.getRoot().getPath()));
        InitializationContextImpl context = new InitializationContextImpl(executionEnvironment,
                null,
                null,
                null);
        dbServerInitializer.perform(context);

        assertThat("Db initialization was started", downstreamContextCaptor.getAllValues().size(), is(0));
    }

    @Test
    public void dbExist_dbInitializationStarted_envUpdated() throws IOException, DatabaseException {
        temporaryFolder.newFolder("db1");
        temporaryFolder.newFolder("db2");

        ArgumentCaptor<InitializationContextImpl> downstreamContextCaptor = ArgumentCaptor.forClass(InitializationContextImpl.class);
        doNothing().when(databaseInitializer).perform(downstreamContextCaptor.capture());

        //act
        ExecutionEnvironmentImpl executionEnvironment = new ExecutionEnvironmentImpl(new DatabaseConfig(temporaryFolder.getRoot().getPath()));
        InitializationContextImpl context = new InitializationContextImpl(executionEnvironment,
                null,
                null,
                null);
        dbServerInitializer.perform(context);

        List<InitializationContextImpl> capturedDbContexts = downstreamContextCaptor.getAllValues();
        assertThat("Db initialization was started not for 2 db", capturedDbContexts.size(), is(2));
        Set<String> dbNames = capturedDbContexts.stream()
                .map(InitializationContextImpl::currentDbContext)
                .map(DatabaseInitializationContext::getDbName)
                .collect(Collectors.toSet());
        assertTrue("Db initialization was started with wrong db name", dbNames.contains("db1"));
        assertTrue("Db initialization was started with wrong db name", dbNames.contains("db2"));
    }
}