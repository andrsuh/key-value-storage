package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.resp.RespReader;
import ru.andrey.kvstorage.resp.RespWriter;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

public class SocketKvsConnection implements KvsConnection {

    private final Socket socket;
    private final RespReader reader;
    private final RespWriter writer;

    public SocketKvsConnection(ConnectionConfig config) {
        try {
            this.socket = new Socket(config.getHost(), config.getPort());
            this.reader = new RespReader(new BufferedInputStream(this.socket.getInputStream()));
            this.writer = new RespWriter(new BufferedOutputStream(this.socket.getOutputStream()));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot connect", e);
        }
    }

    @Override
    public synchronized RespObject send(int commandId, RespObject command) {
        if (socket.isClosed()) {
            throw new IllegalStateException("Socket is closed");
        }
        try {
            writer.write(command);
            RespArray arrayResponse =  (RespArray) reader.readObject();
            System.out.println("Client got response to command id: " + arrayResponse.getObjects().get(0).asString());
            return arrayResponse.getObjects().get(1);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to send command to server");
        }
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot close", e);
        }
    }
}
