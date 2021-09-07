package ru.andrey.kvstorage.resp.object;

import org.junit.Test;
import ru.andrey.kvstorage.resp.object.util.OutputStreamStub;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RespBulkStringTest {

    @Test
    public void getCode_returnValidCode() {
        assertEquals("Wrong code", '$', RespBulkString.CODE);
    }

    @Test
    public void isError_returnFalse() {
        RespBulkString string = new RespBulkString("Example".getBytes(StandardCharsets.UTF_8));
        assertFalse(string.isError());
    }

    @Test
    public void createWithMessage_ReturnValidString() {
        String example = "Example";
        RespBulkString respBulkString = new RespBulkString(example.getBytes(StandardCharsets.UTF_8));
        assertEquals(
                "Passed to constructor and returned strings are not equal",
                example,
                respBulkString.asString()
        );
    }

    @Test
    public void writeString_ValidateWrittenValue() throws IOException {
        String rn = "\r\n";
        String example = "example";
        String expected = "$" +
                example.length() +
                rn +
                example +
                rn;
        RespBulkString respBulkString = new RespBulkString(example.getBytes(StandardCharsets.UTF_8));
        OutputStreamStub outputStreamStub = new OutputStreamStub();
        respBulkString.write(outputStreamStub);
        assertEquals("Written string is not correct", expected, outputStreamStub.getString());
    }

    @Test
    public void writeNullValue_ValidateWrittenValue() throws IOException {
        String expected = "$" + -1 + "\r\n";
        RespBulkString respBulkString = new RespBulkString(null);
        OutputStreamStub outputStreamStub = new OutputStreamStub();
        respBulkString.write(outputStreamStub);
        assertEquals("Written string is not correct", expected, outputStreamStub.getString());
    }
}
