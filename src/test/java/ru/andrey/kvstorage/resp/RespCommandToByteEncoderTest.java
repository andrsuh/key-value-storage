package ru.andrey.kvstorage.resp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import ru.andrey.kvstorage.jclient.command.GetKvsCommand;

import java.util.ArrayList;

@RunWith(MockitoJUnitRunner.class)
public class RespCommandToByteEncoderTest {
    RespCommandToByteEncoder encoder = new RespCommandToByteEncoder();
    ByteToRespCommandDecoder decoder = new ByteToRespCommandDecoder();

    ByteBuf out = Unpooled.buffer();

    @Test
    public void testEncode() throws Exception {
        GetKvsCommand command = new GetKvsCommand("db", "table", "key");

        encoder.encode(null, command.serialize(), out);

        int len = out.readableBytes();
        System.out.println("Client encoded: " + len);
//        byte[] data = new byte[len];
//        out.readBytes(data, 0, len);
//
//        System.out.println(new String(data, StandardCharsets.UTF_8));

        ArrayList<Object> res = new ArrayList<>();
        decoder.decode(null, out, res);

        System.out.println(res.get(0));
    }
}