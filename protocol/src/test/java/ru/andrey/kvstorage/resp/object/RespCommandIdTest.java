package ru.andrey.kvstorage.resp.object;

import org.junit.Test;
import ru.andrey.kvstorage.resp.object.util.IntConverter;
import ru.andrey.kvstorage.resp.object.util.OutputStreamStub;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RespCommandIdTest {

    @Test
    public void getCode_returnValidCode() {
        assertEquals("Wrong code", '!', RespCommandId.CODE);
    }

    @Test
    public void isError_returnFalse() {
        RespCommandId id = new RespCommandId(15);
        assertFalse(id.isError());
    }

    @Test
    public void getAsString_ReturnValidString() {
        int number = 15;
        RespCommandId id = new RespCommandId(15);
        assertEquals("Strings are not equal", String.valueOf(number), id.asString());
    }

    @Test
    public void writeId_ValidateWrittenValue() throws IOException {
        int id = 5;
        String expected = "!" + new String(IntConverter.intToBytes(id)) + "\r\n";
        RespCommandId commandId = new RespCommandId(id);
        OutputStreamStub outputStreamStub = new OutputStreamStub();
        commandId.write(outputStreamStub);
        assertEquals("Written string is not correct", expected, outputStreamStub.getString());
    }
}
