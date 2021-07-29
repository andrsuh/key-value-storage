package ru.andrey.kvstorage.resp.object;

import org.junit.Test;
import org.mockito.Mockito;
import ru.andrey.kvstorage.resp.object.util.IntConverter;
import ru.andrey.kvstorage.resp.object.util.OutputStreamStub;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.*;

public class RespArrayTest {

    private final static String CRLF = "\r\n";

    @Test
    public void getCode_ReturnValidCode() {
        assertEquals("Wrong code", '*', RespArray.CODE);
    }

    @Test
    public void isError_ReturnFalse() {
        RespArray array = new RespArray(Mockito.mock(RespObject.class));
        assertFalse(array.isError());
    }

    @Test
    public void createWithObjectArray_ReturnValidObjects() {
        RespBulkString respBulkString = new RespBulkString("example".getBytes(StandardCharsets.UTF_8));
        RespCommandId respCommandId = new RespCommandId(0);
        RespError respError = new RespError("Error message".getBytes(StandardCharsets.UTF_8));
        RespObject[] objects = { respBulkString, respCommandId, respError };
        RespArray respArray = new RespArray(objects);
        List<RespObject> resultObjects = respArray.getObjects();
        for(var object : objects) {
            assertTrue("Object not found in array", resultObjects.contains(object));
        }
    }

    @Test
    public void writeArray_ValidateWrittenValue() throws IOException {
        RespBulkString respBulkString = new RespBulkString("example".getBytes(StandardCharsets.UTF_8));
        RespCommandId respCommandId = new RespCommandId(0);
        RespError respError = new RespError("Error message".getBytes(StandardCharsets.UTF_8));
        RespObject[] objects = { respBulkString, respCommandId, respError };
        RespArray respArray = new RespArray(objects);
        String expected = "*" + objects.length + CRLF +
                respToString(respBulkString) +
                respToString(respCommandId) +
                respToString(respError);
        OutputStreamStub outputStreamStub = new OutputStreamStub();
        respArray.write(outputStreamStub);
        assertEquals("Written and expected values are not equal", expected, outputStreamStub.getString());
    }

    @Test
    public void writeEmptyArray_ValidateWrittenValue() throws IOException {
        RespArray array = new RespArray();
        assertEquals("Created array is not empty", 0, array.getObjects().size());
        String expected = "*0\r\n";
        OutputStreamStub outputStreamStub = new OutputStreamStub();
        array.write(outputStreamStub);
        assertEquals("Written and expected values are not equal", expected, outputStreamStub.getString());
    }

    private String respToString(RespBulkString object) {
        return "$" +
                object.asString().length() +
                CRLF +
                object.asString() +
                CRLF;
    }

    private String respToString(RespCommandId object) {
        return "!" + new String(IntConverter.intToBytes(Integer.parseInt(object.asString()))) + "\r\n";
    }

    private String respToString(RespError object) {
        return "-" + object.asString() + "\r\n";
    }
}
