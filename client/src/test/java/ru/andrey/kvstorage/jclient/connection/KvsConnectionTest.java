package ru.andrey.kvstorage.jclient.connection;

import org.junit.Test;
import org.mockito.Mockito;
import ru.andrey.kvstorage.jclient.exception.ConnectionException;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.server.DatabaseServer;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

public class KvsConnectionTest {

    @Test
    public void sendRespArray_VerifyDatabaseServerMethodCalled() throws ConnectionException {
        DatabaseServer databaseServer = mock(DatabaseServer.class);
        when(databaseServer.executeNextCommand(any(RespArray.class))).thenReturn(
                CompletableFuture.supplyAsync(() -> Mockito.mock(DatabaseCommandResult.class))
        );
        DirectReferenceKvsConnection connection = new DirectReferenceKvsConnection(databaseServer);
        connection.send(0, new RespArray());
        verify(databaseServer, times(1)).executeNextCommand(any(RespArray.class));
    }
}
