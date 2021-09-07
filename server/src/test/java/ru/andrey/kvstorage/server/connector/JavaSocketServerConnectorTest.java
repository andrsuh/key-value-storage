package ru.andrey.kvstorage.server.connector;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespCommandId;
import ru.andrey.kvstorage.server.DatabaseServer;
import ru.andrey.kvstorage.server.config.ServerConfig;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.DatabaseCommands;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class JavaSocketServerConnectorTest {

    private static final DatabaseCommandResult SUCCESS = DatabaseCommandResult.success("success".getBytes(StandardCharsets.UTF_8));
    private static final DatabaseCommandResult ERROR = DatabaseCommandResult.error("error");

    private DatabaseServer server;
    private JavaSocketServerConnector connector;
    private ServerSocket occupiedSocket;

    @Before
    public void setUp() {
        server = mock(DatabaseServer.class);
        when(server.executeNextCommand((DatabaseCommand) any()))
                .thenReturn(CompletableFuture.completedFuture(SUCCESS));
    }

    @After
    public void tearDown() {
        if (occupiedSocket != null && !occupiedSocket.isClosed()) {
            try {
                occupiedSocket.close();
            } catch (Exception ignored) {
            }
        }
        occupiedSocket = null;
        if (connector != null) {
            try {
                connector.close();
            } catch (Exception ignored) {
            }
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void ctor_WhenValidParams_OccupyPort() throws IOException {
        ServerConfig config = new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT);
        connector = new JavaSocketServerConnector(server, config);
        assertFalse("Port is free when connector was created", isPortAvailable(ServerConfig.DEFAULT_PORT));
    }

    @Test
    public void ctor_WhenPortIsUsed_ThrowException() throws IOException {
        occupiedSocket = new ServerSocket(ServerConfig.DEFAULT_PORT);
        ServerConfig config = new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT);
        assertThrows("IOException was not thrown though port is occupied",
                IOException.class, () -> connector = new JavaSocketServerConnector(server, config));
    }

    @Test
    public void ctor_WhenRandomPort_OccupyPort() throws IOException {
        ServerConfig config = new ServerConfig(ServerConfig.DEFAULT_HOST, 0);
        connector = new JavaSocketServerConnector(server, config);
    }

    @Test
    public void startAndAccept_WhenClientConnects_ConnectionIsHandledInClientIOWorkers() throws Exception {
        ServerConfig config = new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT);
        connector = new JavaSocketServerConnector(server, config);

        ExecutorService executor = mockExecutorService();

        Field clientExecutorField = connector.getClass().getDeclaredField("clientIOWorkers");
        clientExecutorField.setAccessible(true);
        clientExecutorField.set(connector, executor);

        connector.start();
        try (Socket ignored = new Socket(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            }
            verify(executor, times(1)).submit((Runnable) any());
        }

        connector.close();
        connector = null;
    }

    @Test
    public void startAndAccept_WhenClientSendsRequest_ReturnResult() throws IOException {
        ServerConfig config = new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT);
        connector = new JavaSocketServerConnector(server, config);
        connector.start();

        try (Socket client = new Socket(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT);
             var input = new BufferedInputStream(client.getInputStream());
             var output = new BufferedOutputStream(client.getOutputStream())) {

            RespArray array = new RespArray(
                    new RespCommandId(0),
                    new RespBulkString(DatabaseCommands.GET_KEY.name().getBytes(StandardCharsets.UTF_8)),
                    new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                    new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                    new RespBulkString("key".getBytes(StandardCharsets.UTF_8)));
            array.write(output);
            output.flush();

            ByteArrayOutputStream arrayOutput = new ByteArrayOutputStream();
            SUCCESS.serialize().write(arrayOutput);
            byte[] expected = arrayOutput.toByteArray();

            assertArrayEquals("Data read from server is not valid",
                    expected, input.readNBytes(expected.length));
        }
    }

    @Test
    public void startAndAccept_WhenExecutionError_ReturnErrorToClient() throws IOException {
        when(server.executeNextCommand(any(DatabaseCommand.class)))
                .thenReturn(CompletableFuture.completedFuture(ERROR));

        ServerConfig config = new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT);
        connector = new JavaSocketServerConnector(server, config);
        connector.start();

        try (Socket client = new Socket(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT);
             var input = new BufferedInputStream(client.getInputStream());
             var output = new BufferedOutputStream(client.getOutputStream())) {

            RespArray array = new RespArray(
                    new RespCommandId(0),
                    new RespBulkString(DatabaseCommands.GET_KEY.name().getBytes(StandardCharsets.UTF_8)),
                    new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                    new RespBulkString("TEST".getBytes(StandardCharsets.UTF_8)),
                    new RespBulkString("key".getBytes(StandardCharsets.UTF_8)));
            array.write(output);
            output.flush();

            ByteArrayOutputStream arrayOutput = new ByteArrayOutputStream();
            ERROR.serialize().write(arrayOutput);
            byte[] expected = arrayOutput.toByteArray();

            assertArrayEquals("Data read from server is not valid",
                    expected, input.readNBytes(expected.length));
        }
    }

    @Test
    public void close_WhenNotStarted_CloseSocket() throws Exception {
        ServerConfig config = new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT);
        connector = new JavaSocketServerConnector(server, config);

        ExecutorService acceptorExecutor = mockExecutorService();
        Field acceptorExecutorField = connector.getClass().getDeclaredField("connectionAcceptorExecutor");
        acceptorExecutorField.setAccessible(true);
        acceptorExecutorField.set(connector, acceptorExecutor);

        ExecutorService clientExecutor = mockExecutorService();
        Field clientExecutorField = connector.getClass().getDeclaredField("clientIOWorkers");
        clientExecutorField.setAccessible(true);
        clientExecutorField.set(connector, clientExecutor);

        connector.close();

        assertTrue("Port is not available after connector close", isPortAvailable(ServerConfig.DEFAULT_PORT));
        verify(acceptorExecutor, times(1)).shutdownNow();
        verify(clientExecutor, times(1)).shutdownNow();
    }

    @Test
    public void close_WhenStarted_CloseSocketAndStopExecutors() throws Exception {
        ServerConfig config = new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT);
        connector = new JavaSocketServerConnector(server, config);

        ExecutorService acceptorExecutor = mock(ExecutorService.class);
        when(acceptorExecutor.submit((Runnable) any())).thenReturn(CompletableFuture.completedFuture(null));
        when(acceptorExecutor.submit((Callable<?>) any())).thenAnswer(invocation -> acceptorExecutor.submit(() -> {}));
        doNothing().when(acceptorExecutor).shutdown();
        when(acceptorExecutor.shutdownNow()).thenReturn(Collections.emptyList());
        Field acceptorExecutorField = connector.getClass().getDeclaredField("connectionAcceptorExecutor");
        acceptorExecutorField.setAccessible(true);
        acceptorExecutorField.set(connector, acceptorExecutor);

        ExecutorService clientExecutor = mock(ExecutorService.class);
        when(clientExecutor.submit((Runnable) any())).thenReturn(CompletableFuture.completedFuture(null));
        when(clientExecutor.submit((Callable<?>) any())).thenAnswer(invocation -> clientExecutor.submit(() -> {}));
        doNothing().when(clientExecutor).shutdown();
        when(clientExecutor.shutdownNow()).thenReturn(Collections.emptyList());
        Field clientExecutorField = connector.getClass().getDeclaredField("clientIOWorkers");
        clientExecutorField.setAccessible(true);
        clientExecutorField.set(connector, clientExecutor);

        connector.close();

        verify(acceptorExecutor, times(1)).shutdownNow();
        verify(clientExecutor, times(1)).shutdownNow();
    }

    private ExecutorService mockExecutorService() {
        ExecutorService executor = mock(ExecutorService.class);
        when(executor.submit((Runnable) any())).thenReturn(CompletableFuture.completedFuture(null));
        when(executor.submit((Callable<?>) any())).thenAnswer(invocation -> executor.submit(() -> {}));
        doNothing().when(executor).shutdown();
        when(executor.shutdownNow()).thenReturn(Collections.emptyList());
        return executor;
    }

    private static boolean isPortAvailable(int port) {
        try (Socket ignored = new Socket("localhost", port)) {
            return false;
        } catch (IOException ignored) {
            return true;
        }
    }
}