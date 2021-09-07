package ru.andrey.kvstorage.resp.object;

import org.junit.Test;
import ru.andrey.kvstorage.resp.object.util.OutputStreamStub;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RespErrorTest {

    private static final String message = "Example message for resp error";

    @Test
    public void getCode_ReturnValidCode() {
        assertEquals("Wrong code", '-', RespError.CODE);
    }

    @Test
    public void isError_ReturnTrue() {
        assertTrue(new RespError(message.getBytes(StandardCharsets.UTF_8)).isError());
    }

    @Test
    public void createWithMessage_ReturnValidString() {
        RespError error = new RespError(message.getBytes(StandardCharsets.UTF_8));
        assertEquals(
                "Passed to constructor and returned messages are not equal",
                message,
                error.asString()
        );
    }

    @Test
    public void writeError_ValidateWrittenValue() throws IOException {
        RespError error = new RespError(message.getBytes(StandardCharsets.UTF_8));
        String expected = "-" + new String(message.getBytes(StandardCharsets.UTF_8)) + "\r\n";
        OutputStreamStub outputStreamStub = new OutputStreamStub();
        error.write(outputStreamStub);
        assertEquals("Written string is not correct", expected, outputStreamStub.getString());
    }
}
