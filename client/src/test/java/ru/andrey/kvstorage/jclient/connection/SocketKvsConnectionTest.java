package ru.andrey.kvstorage.jclient.connection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.andrey.kvstorage.jclient.exception.ConnectionException;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespCommandId;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class SocketKvsConnectionTest {

    private ServerSocket serverSocket;

    @Before
    public void setUp() throws IOException {
        serverSocket = new ServerSocket(8080);
    }

    @After
    public void tearDown() throws IOException {
        if (!serverSocket.isClosed())
            serverSocket.close();
    }

    @Test
    public void connectToEmptyPort_ThrowException() {
        assertThrows(Exception.class, () -> {
            new SocketKvsConnection(
                    new ConnectionConfig("localhost", 8088)
            );
        });
    }

    @Test
    public void connectToNonEmptyHost() {
        new SocketKvsConnection(
                new ConnectionConfig("localhost", 8080)
        );
    }

    @Test
    public void sendToClosedPort_ThrowException() throws IOException {
        RespArray array = new RespArray(
                new RespCommandId(1),
                new RespBulkString("db1".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("table1".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("key1".getBytes(StandardCharsets.UTF_8))
        );
        SocketKvsConnection connection = new SocketKvsConnection(
                new ConnectionConfig("localhost", 8080)
        );
        serverSocket.close();
        assertThrows(ConnectionException.class, () -> connection.send(1, array));
    }

    @Test
    public void sendToServer_WriteCorrectInfo() throws ConnectionException, IOException {
        RespArray array = new RespArray(
                new RespCommandId(1),
                new RespBulkString("db1".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("table1".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("key1".getBytes(StandardCharsets.UTF_8))
        );
        SocketKvsConnection connection = new SocketKvsConnection(
                new ConnectionConfig("localhost", 8080)
        );
        String message = "success";
        String result = "$" + message.length() + "\r\n" + message + "\r\n";
        Runnable task = () -> {
            try {
                Socket clientSocket = serverSocket.accept();
                clientSocket.getOutputStream().write(result.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException();
            }
        };
        new Thread(task).start();
        RespObject object = connection.send(1, array);
        assertThat(object, instanceOf(RespBulkString.class));
        assertEquals(message, object.asString());
    }

    @Test
    public void sendToServer_ServerReturnedIncorrectResponse_ThrowException() {
        RespArray array = new RespArray(
                new RespCommandId(1),
                new RespBulkString("db1".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("table1".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("key1".getBytes(StandardCharsets.UTF_8))
        );
        SocketKvsConnection connection = new SocketKvsConnection(
                new ConnectionConfig("localhost", 8080)
        );
        Runnable task = () -> {
            try {
                Socket clientSocket = serverSocket.accept();
                clientSocket.getOutputStream().write("$6\r\nsuccess\r\n".getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException();
            }
        };
        new Thread(task).start();
        assertThrows(Exception.class, () -> connection.send(1, array));
    }
}
