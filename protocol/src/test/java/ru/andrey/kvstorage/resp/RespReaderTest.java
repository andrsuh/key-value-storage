package ru.andrey.kvstorage.resp;

import org.junit.Test;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespCommandId;
import ru.andrey.kvstorage.resp.object.RespError;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class RespReaderTest {

    @Test
    public void hasArray_ReturnTrue() throws IOException {
        InputStreamStub stub = new InputStreamStub("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        RespReader reader = new RespReader(stub);
        assertTrue("Passed object is array, but method returns false", reader.hasArray());
    }

    @Test
    public void hasArray_WhenNotArray_ReturnFalse() throws IOException {
        InputStreamStub stub = new InputStreamStub("$6\r\nfoobar\r\n");
        RespReader reader = new RespReader(stub);
        assertFalse("Passed object is not array, but method returns true", reader.hasArray());
    }

    @Test
    public void readBulkString_ReturnValidObject() throws IOException {
        InputStreamStub stub = new InputStreamStub("$6\r\nfoobar\r\n");
        RespReader reader = new RespReader(stub);
        RespObject object = reader.readObject();
        assertThat("Returned object is not bulk string", object, instanceOf(RespBulkString.class));
    }

    @Test
    public void readCommandId_ReturnValidObject() throws IOException {
        InputStreamStub stub = new InputStreamStub("!" + new String(intToByteArray(1)) + "\r\n");
        RespReader reader = new RespReader(stub);
        RespObject object = reader.readObject();
        assertThat("Returned object is not command id", object, instanceOf(RespCommandId.class));
    }

    @Test
    public void readError_ReturnValidObject() throws IOException {
        InputStreamStub stub = new InputStreamStub("-Error message\r\n");
        RespReader reader = new RespReader(stub);
        RespObject object = reader.readObject();
        assertThat("Returned object is not resp error", object, instanceOf(RespError.class));
    }

    @Test
    public void readInvalidObject_ThrowException() {
        InputStreamStub stub = new InputStreamStub("!" + new String(intToByteArray(1)) + "\r");
        RespReader reader = new RespReader(stub);
        assertThrows(Exception.class, reader::readObject);
    }

    @Test
    public void readUnknownObject_ThrowIOException() {
        InputStreamStub stub = new InputStreamStub("/////////");
        RespReader reader = new RespReader(stub);
        assertThrows(IOException.class, reader::readObject);
    }

    public static byte[] intToByteArray(int value) {
        return new byte[] {
                (byte)(value >>> 24),
                (byte)(value >>> 16),
                (byte)(value >>> 8),
                (byte)value};
    }

    private static class InputStreamStub extends InputStream {

        private final String data;
        private int counter = 0;

        public InputStreamStub(String data) {
            this.data = data;
        }

        @Override
        public int read() throws IOException {
            if (counter >= data.length())
                return -1;
            return data.charAt(counter++);
        }

        @Override
        public int available() {
            return data.length() - counter;
        }


    }
}
