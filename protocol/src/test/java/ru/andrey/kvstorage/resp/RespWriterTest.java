package ru.andrey.kvstorage.resp;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class RespWriterTest {

    @Test
    public void close_CallStreamCloseMethod() throws IOException {
        OutputStream stream = mock(OutputStream.class);
        RespWriter writer = new RespWriter(stream);
        writer.close();
        verify(stream, times(1)).close();
    }

    @Test
    public void write_CallObjectWriteMethod() throws IOException {
        OutputStream stream = mock(OutputStream.class);
        RespWriter writer = new RespWriter(stream);
        RespObject object = mock(RespObject.class);
        ArgumentCaptor<OutputStream> streamArgumentCaptor = ArgumentCaptor.forClass(OutputStream.class);
        writer.write(object);
        verify(object).write(streamArgumentCaptor.capture());
        verify(object, times(1)).write(any());
        assertEquals("Wrong stream passed to RespObject", stream, streamArgumentCaptor.getValue());
    }
}
