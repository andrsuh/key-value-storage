package ru.andrey.kvstorage.server.console.impl;

import org.junit.Test;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class SuccessDatabaseCommandResultTest {

    @Test
    public void isSuccess_ReturnFalse() {
        assertTrue("SuccessDatabaseCommandResult is not success",
                new SuccessDatabaseCommandResult("example".getBytes(StandardCharsets.UTF_8)).isSuccess());
    }

    @Test
    public void createWithNullPayload_ReturnNullPayload() {
        assertNull(
                "Payload must be null",
                new SuccessDatabaseCommandResult(null).getPayLoad()
        );
    }

    @Test
    public void getPayload_ReturnPayload() {
        String payload = "examplepayload";
        assertEquals(
                "Passed to constructor and returned payloads are not equal",
                payload,
                new SuccessDatabaseCommandResult(payload.getBytes(StandardCharsets.UTF_8)).getPayLoad()
        );
    }

    @Test
    public void serialize_WhenContainsPayload_ReturnRespError() {
        String payload = "Test message";
        var result = new SuccessDatabaseCommandResult(payload.getBytes(StandardCharsets.UTF_8));
        RespObject respObject = result.serialize();
        assertThat("Serialize result is not bulk string", respObject, instanceOf(RespBulkString.class));
        assertEquals(payload, result.serialize().asString());
    }
}
