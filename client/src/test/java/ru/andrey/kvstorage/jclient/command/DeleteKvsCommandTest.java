package ru.andrey.kvstorage.jclient.command;

import org.junit.Test;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespCommandId;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DeleteKvsCommandTest {

    private final String dbName = "database";
    private final String tableName = "table";
    private final String key = "key";

    @Test
    public void serializeCommand_ReturnArrayWithCorrectSize() {
        DeleteKvsCommand command = new DeleteKvsCommand(dbName, tableName, key);
        RespArray array = command.serialize();
        List<RespObject> objects = array.getObjects();
        assertEquals("Wrong array size", 5, objects.size());
    }

    @Test
    public void serializeCommand_ReturnArrayWithIdObject() {
        DeleteKvsCommand command = new DeleteKvsCommand(dbName, tableName, key);
        RespArray array = command.serialize();
        List<RespObject> objects = array.getObjects();
        assertThat("First object in resp array is not id", objects.get(0), instanceOf(RespCommandId.class));
        RespCommandId id = (RespCommandId) objects.get(0);
        assertNotNull("Command id in array is null", id);
        assertEquals("Id in array and id from command does not match",
                command.getCommandId(),
                Integer.valueOf(id.asString()).intValue()
        );
    }

    @Test
    public void serializeCommand_ReturnArrayWithCommandNameObject() {
        DeleteKvsCommand command = new DeleteKvsCommand(dbName, tableName, key);
        RespArray array = command.serialize();
        List<RespObject> objects = array.getObjects();
        assertThat("Second object in resp array is not string", objects.get(1), instanceOf(RespBulkString.class));
        RespBulkString commandName = (RespBulkString) objects.get(1);
        assertEquals("Wrong command name in array", "DELETE_KEY", commandName.asString());
    }

    @Test
    public void serializeCommand_ReturnArrayWithDatabaseNameObject() {
        DeleteKvsCommand command = new DeleteKvsCommand(dbName, tableName, key);
        RespArray array = command.serialize();
        List<RespObject> objects = array.getObjects();
        assertThat("Third object in resp array is not string", objects.get(2), instanceOf(RespBulkString.class));
        RespBulkString dbNameString = (RespBulkString) objects.get(2);
        assertEquals("Wrong database name in array", dbName, dbNameString.asString());
    }

    @Test
    public void serializeCommand_ReturnArrayWithTableNameObject() {
        DeleteKvsCommand command = new DeleteKvsCommand(dbName, tableName, key);
        RespArray array = command.serialize();
        List<RespObject> objects = array.getObjects();
        assertThat("Fourth object in resp array is not string", objects.get(3), instanceOf(RespBulkString.class));
        RespBulkString tableNameString = (RespBulkString) objects.get(3);
        assertEquals("Wrong table name in array", tableName, tableNameString.asString());
    }

    @Test
    public void serializeCommand_ReturnArrayWithKeyObject() {
        DeleteKvsCommand command = new DeleteKvsCommand(dbName, tableName, key);
        RespArray array = command.serialize();
        List<RespObject> objects = array.getObjects();
        assertThat("Fifth object in resp array is not string", objects.get(4), instanceOf(RespBulkString.class));
        RespBulkString keyString = (RespBulkString) objects.get(4);
        assertEquals("Wrong key in array", key, keyString.asString());
    }
}
