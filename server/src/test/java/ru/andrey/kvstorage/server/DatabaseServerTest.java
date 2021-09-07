package ru.andrey.kvstorage.server;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import ru.andrey.kvstorage.server.config.DatabaseConfig;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.initialization.InitializationContext;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

public class DatabaseServerTest {

    @Rule
    public final TemporaryFolder dbRootFolder = new TemporaryFolder();

    @Test
    public void initialize_VerifyPerformCalledWithCorrectArgs() throws DatabaseException {
        ArgumentCaptor<InitializationContext> contextCaptor = ArgumentCaptor.forClass(InitializationContext.class);
        DatabaseServerInitializer initializer = mock(DatabaseServerInitializer.class);
        ExecutionEnvironment environment = new ExecutionEnvironmentImpl(
                new DatabaseConfig(dbRootFolder.getRoot().getPath())
        );

        DatabaseServer.initialize(environment, initializer);

        verify(initializer).perform(contextCaptor.capture());
        verify(initializer, times(1)).perform(any(InitializationContext.class));
        InitializationContext context = contextCaptor.getValue();
        assertEquals(
                "Initial and passed to perform() environments are not equal",
                environment,
                context.executionEnvironment()
        );
        assertNull("Db context must be null", context.currentDbContext());
        assertNull("Table context must be null", context.currentTableContext());
        assertNull("Segment context must be null", context.currentSegmentContext());
    }

    @Test
    public void executeCommand_ValidateResult() throws DatabaseException, ExecutionException, InterruptedException, TimeoutException {
        DatabaseServer server = DatabaseServer.initialize(
                mock(ExecutionEnvironment.class),
                mock(DatabaseServerInitializer.class)
        );
        DatabaseCommand command = mock(DatabaseCommand.class);
        server.executeNextCommand(command).get(1, TimeUnit.SECONDS);

        verify(command, times(1)).execute();
    }
}
