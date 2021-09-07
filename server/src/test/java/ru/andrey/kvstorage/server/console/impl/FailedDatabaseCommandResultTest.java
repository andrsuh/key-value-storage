package ru.andrey.kvstorage.server.console.impl;

import org.junit.Assert;
import org.junit.Test;
import ru.andrey.kvstorage.resp.object.RespError;
import ru.andrey.kvstorage.resp.object.RespObject;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class FailedDatabaseCommandResultTest {

    @Test
    public void isSuccess_ReturnFalse() {
        assertFalse("FailedDatabaseCommandResult is success while it is not",
                new FailedDatabaseCommandResult("").isSuccess());
    }

    @Test
    public void createWithNullPayload_ReturnNullPayload() {
        assertNull(
                "Payload must be null",
                new FailedDatabaseCommandResult(null).getPayLoad()
        );
    }

    @Test
    public void getPayload_ReturnPayload() {
        String payload = "examplepayload";
        assertEquals(
                "Passed to constructor and returned payloads are not equal",
                payload,
                new FailedDatabaseCommandResult(payload).getPayLoad()
        );
    }

    @Test
    public void serialize_WhenContainsPayload_ReturnRespError() {
        String payload = "Test error";
        var result = new FailedDatabaseCommandResult(payload);
        RespObject respObject = result.serialize();
        assertThat("Serialize result is not resp error", respObject, instanceOf(RespError.class));
        Assert.assertEquals(payload, result.serialize().asString());
    }
}