package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.jclient.exception.KvsConnectionException;
import ru.andrey.kvstorage.resp.RespReader;
import ru.andrey.kvstorage.resp.RespWriter;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

public class SocketConnection implements KvsConnection {

    private final Socket socket;
    private final RespReader reader;
    private final RespWriter writer;

    public SocketConnection() {
        try {
            this.socket = new Socket("127.0.0.1", 4321);// todo sukhoa remove magic constant
            this.reader = new RespReader(new BufferedInputStream(this.socket.getInputStream()));
            this.writer = new RespWriter(new BufferedOutputStream(this.socket.getOutputStream()));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot connect", e);
        }
    }

    @Override
    public RespObject send(RespObject object) {
        if (socket.isClosed()) {
            throw new IllegalStateException("Socket is closed");
        }
        try {
            writer.write(object);
            return reader.readObject();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to send command to server");
        }
    }

    @Override
    public void close() throws KvsConnectionException {
        try {
            socket.close();
        } catch (IOException e) {
            throw new KvsConnectionException("Cannot close", e);
        }
    }
}
