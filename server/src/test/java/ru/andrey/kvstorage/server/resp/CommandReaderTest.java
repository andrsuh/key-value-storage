package ru.andrey.kvstorage.server.resp;

import org.junit.Test;
import ru.andrey.kvstorage.resp.RespReader;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespCommandId;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.console.impl.DeleteKeyCommand;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class CommandReaderTest {

    @Test
    public void readerWithNoCommands_HasNextCommand_ReturnFalse() throws IOException {
        RespReader readerMock = mock(RespReader.class);
        when(readerMock.hasArray()).thenReturn(false);
        CommandReader reader = new CommandReader(readerMock, mock(ExecutionEnvironment.class));
        assertFalse("Result must be false, but it's not", reader.hasNextCommand());
    }

    @Test
    public void readerWithCommands_HasNextCommand_ReturnTrue() throws IOException {
        RespReader readerMock = mock(RespReader.class);
        when(readerMock.hasArray()).thenReturn(true);
        CommandReader reader = new CommandReader(readerMock, mock(ExecutionEnvironment.class));
        assertTrue("Result must be true, but it's not", reader.hasNextCommand());
    }

    @Test
    public void close_CallReaderCloseMethod() throws Exception {
        RespReader readerMock = mock(RespReader.class);
        CommandReader reader = new CommandReader(readerMock, mock(ExecutionEnvironment.class));
        reader.close();
        verify(readerMock, times(1)).close();
    }

    @Test
    public void readCommandWithNoObjects_ThrowException() throws IOException {
        RespReader readerMock = mock(RespReader.class);
        when(readerMock.readArray()).thenReturn(new RespArray());
        CommandReader reader = new CommandReader(readerMock, mock(ExecutionEnvironment.class));
        assertThrows(IllegalArgumentException.class, reader::readCommand);
    }

    @Test
    public void readCommandWithoutId_ThrowException() throws IOException {
        RespArray array = new RespArray(
                new RespBulkString("DELETE_KEY".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("db1".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("table1".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("key1".getBytes(StandardCharsets.UTF_8))
        );
        RespReader respReader = mock(RespReader.class);
        when(respReader.readArray()).thenReturn(array);
        CommandReader commandReader = new CommandReader(respReader, mock(ExecutionEnvironment.class));
        assertThrows(IllegalArgumentException.class, commandReader::readCommand);
    }

    @Test
    public void readCommandWithoutCommandName_ThrowException() throws IOException {
        RespArray array = new RespArray(
                new RespCommandId(1),
                new RespBulkString("db1".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("table1".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("key1".getBytes(StandardCharsets.UTF_8))
        );
        RespReader respReader = mock(RespReader.class);
        when(respReader.readArray()).thenReturn(array);
        CommandReader commandReader = new CommandReader(respReader, mock(ExecutionEnvironment.class));
        assertThrows(IllegalArgumentException.class, commandReader::readCommand);
    }

    @Test
    public void readCommand_ReturnValidCommand() throws IOException {
        RespArray array = new RespArray(
                new RespCommandId(1),
                new RespBulkString("DELETE_KEY".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("db1".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("table1".getBytes(StandardCharsets.UTF_8)),
                new RespBulkString("key1".getBytes(StandardCharsets.UTF_8))
        );
        RespReader reader = mock(RespReader.class);
        when(reader.readArray()).thenReturn(array);
        CommandReader commandReader = new CommandReader(reader, mock(ExecutionEnvironment.class));
        DatabaseCommand command = commandReader.readCommand();
        assertThat("Command reader returned wrong command", command, instanceOf(DeleteKeyCommand.class));
    }
}
