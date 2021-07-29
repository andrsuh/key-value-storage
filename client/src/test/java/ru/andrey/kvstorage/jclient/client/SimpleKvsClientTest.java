package ru.andrey.kvstorage.jclient.client;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import ru.andrey.kvstorage.jclient.connection.KvsConnection;
import ru.andrey.kvstorage.jclient.exception.ConnectionException;
import ru.andrey.kvstorage.jclient.exception.DatabaseExecutionException;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

public class SimpleKvsClientTest {

    private final String dbName = "database";
    private final String tableName = "table";
    private final String key = "key";
    private final String value = "value";

    @Test
    public void executeCommand_ReturnValidResponse() throws ConnectionException, DatabaseExecutionException {
        KvsConnection connection = mock(KvsConnection.class);
        when(connection.send(anyInt(), any(RespArray.class))).thenReturn(new SuccessRespObjectStub());
        SimpleKvsClient client = new SimpleKvsClient(dbName, () -> connection);
        assertEquals("exampleresponse", client.createDatabase());
    }

    @Test
    public void executeFailedCommand_ThrowException() throws ConnectionException {
        KvsConnection connection = mock(KvsConnection.class);
        when(connection.send(anyInt(), any(RespArray.class))).thenReturn(new FailedRespObjectStub());
        SimpleKvsClient client = new SimpleKvsClient(dbName, () -> connection);
        assertThrows(DatabaseExecutionException.class, client::createDatabase);
    }

    @Test
    public void createDatabase_SendCorrectCommand() throws ConnectionException, DatabaseExecutionException {
        KvsConnection connection = mock(KvsConnection.class);
        when(connection.send(anyInt(), any(RespArray.class))).thenReturn(new SuccessRespObjectStub());
        ArgumentCaptor<RespArray> arrayArgumentCaptor = ArgumentCaptor.forClass(RespArray.class);
        SimpleKvsClient client = new SimpleKvsClient(dbName, () -> connection);
        client.createDatabase();
        verify(connection).send(anyInt(), arrayArgumentCaptor.capture());
        RespArray array = arrayArgumentCaptor.getValue();
        checkArray(array, 3);
        List<RespObject> objects = array.getObjects();
        assertEquals(
                "Wrong command name in array passed to connection",
                "CREATE_DATABASE",
                objects.get(1).asString()
        );
        assertEquals(
                "Wrong database name in array passed to connection",
                dbName,
                objects.get(2).asString()
        );
    }

    @Test
    public void createTable_SendCorrectCommand() throws ConnectionException, DatabaseExecutionException {
        KvsConnection connection = mock(KvsConnection.class);
        when(connection.send(anyInt(), any(RespArray.class))).thenReturn(new SuccessRespObjectStub());
        ArgumentCaptor<RespArray> arrayArgumentCaptor = ArgumentCaptor.forClass(RespArray.class);
        SimpleKvsClient client = new SimpleKvsClient(dbName, () -> connection);
        client.createTable(tableName);
        verify(connection).send(anyInt(), arrayArgumentCaptor.capture());
        RespArray array = arrayArgumentCaptor.getValue();
        checkArray(array, 4);
        List<RespObject> objects = array.getObjects();
        assertEquals(
                "Wrong command name in array passed to connection",
                "CREATE_TABLE",
                objects.get(1).asString()
        );
        assertEquals(
                "Wrong database name in array passed to connection",
                dbName,
                objects.get(2).asString()
        );
        assertEquals(
                "Wrong table name in array passed to connection",
                tableName,
                objects.get(3).asString()
        );
    }

    @Test
    public void getKey_SendCorrectArray() throws ConnectionException, DatabaseExecutionException {
        KvsConnection connection = mock(KvsConnection.class);
        when(connection.send(anyInt(), any(RespArray.class))).thenReturn(new SuccessRespObjectStub());
        ArgumentCaptor<RespArray> arrayArgumentCaptor = ArgumentCaptor.forClass(RespArray.class);
        SimpleKvsClient client = new SimpleKvsClient(dbName, () -> connection);
        client.get(tableName, key);
        verify(connection).send(anyInt(), arrayArgumentCaptor.capture());
        RespArray array = arrayArgumentCaptor.getValue();
        checkArray(array, 5);
        List<RespObject> objects = array.getObjects();
        assertEquals(
                "Wrong command name in array passed to connection",
                "GET_KEY",
                objects.get(1).asString()
        );
        assertEquals(
                "Wrong database name in array passed to connection",
                dbName,
                objects.get(2).asString()
        );
        assertEquals(
                "Wrong table name in array passed to connection",
                tableName,
                objects.get(3).asString()
        );
        assertEquals(
                "Wrong key in array passed to connection",
                key,
                objects.get(4).asString()
        );
    }

    @Test
    public void deleteKey_SendCorrectArray() throws ConnectionException, DatabaseExecutionException {
        KvsConnection connection = mock(KvsConnection.class);
        when(connection.send(anyInt(), any(RespArray.class))).thenReturn(new SuccessRespObjectStub());
        ArgumentCaptor<RespArray> arrayArgumentCaptor = ArgumentCaptor.forClass(RespArray.class);
        SimpleKvsClient client = new SimpleKvsClient(dbName, () -> connection);
        client.delete(tableName, key);
        verify(connection).send(anyInt(), arrayArgumentCaptor.capture());
        RespArray array = arrayArgumentCaptor.getValue();
        checkArray(array, 5);
        List<RespObject> objects = array.getObjects();
        assertEquals(
                "Wrong command name in array passed to connection",
                "DELETE_KEY",
                objects.get(1).asString()
        );
        assertEquals(
                "Wrong database name in array passed to connection",
                dbName,
                objects.get(2).asString()
        );
        assertEquals(
                "Wrong table name in array passed to connection",
                tableName,
                objects.get(3).asString()
        );
        assertEquals(
                "Wrong key in array passed to connection",
                key,
                objects.get(4).asString()
        );
    }

    @Test
    public void setKey_SendCorrectArray() throws ConnectionException, DatabaseExecutionException {
        KvsConnection connection = mock(KvsConnection.class);
        when(connection.send(anyInt(), any(RespArray.class))).thenReturn(new SuccessRespObjectStub());
        ArgumentCaptor<RespArray> arrayArgumentCaptor = ArgumentCaptor.forClass(RespArray.class);
        SimpleKvsClient client = new SimpleKvsClient(dbName, () -> connection);
        client.set(tableName, key, value);
        verify(connection).send(anyInt(), arrayArgumentCaptor.capture());
        RespArray array = arrayArgumentCaptor.getValue();
        checkArray(array, 6);
        List<RespObject> objects = array.getObjects();
        assertEquals(
                "Wrong command name in array passed to connection",
                "SET_KEY",
                objects.get(1).asString()
        );
        assertEquals(
                "Wrong database name in array passed to connection",
                dbName,
                objects.get(2).asString()
        );
        assertEquals(
                "Wrong table name in array passed to connection",
                tableName,
                objects.get(3).asString()
        );
        assertEquals(
                "Wrong key in array passed to connection",
                key,
                objects.get(4).asString()
        );
        assertEquals(
                "Wrong value in array passed to connection",
                value,
                objects.get(5).asString()
        );
    }

    public void checkArray(RespArray array, int args) {
        List<RespObject> objects = array.getObjects();
        assertEquals("Wrong amount of resp objects passed to connection", args, objects.size());
        for (int i = 1; i < args; i++)
            assertThat("Wrong object type in array on position " + i , objects.get(i), instanceOf(RespBulkString.class));
    }

    private static class SuccessRespObjectStub implements RespObject {
        @Override
        public boolean isError() {
            return false;
        }

        @Override
        public String asString() {
            return "exampleresponse";
        }

        @Override
        public void write(OutputStream os) throws IOException {

        }
    }

    private static class FailedRespObjectStub implements RespObject {
        @Override
        public boolean isError() {
            return true;
        }

        @Override
        public String asString() {
            return null;
        }

        @Override
        public void write(OutputStream os) throws IOException {

        }
    }
}
