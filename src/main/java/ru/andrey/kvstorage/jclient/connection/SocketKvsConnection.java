package ru.andrey.kvstorage.jclient.connection;

import lombok.extern.slf4j.Slf4j;
import ru.andrey.kvstorage.jclient.exception.KvsConnectionException;
import ru.andrey.kvstorage.resp.RespReader;
import ru.andrey.kvstorage.resp.RespWriter;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

@Slf4j
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
            log.error("Can't create socket connection", e);
            throw new IllegalStateException("Cannot connect", e);
        }
    }

    @Override
    public RespObject send(RespObject object) {
        if (socket.isClosed()) {
            log.error("Can't send response. Socket is closed");
            throw new IllegalStateException("Socket is closed");
        }
        try {
            writer.write(object);
            return reader.readObject();
        } catch (IOException e) {
            log.error("Failed to send command to server", e);
            throw new IllegalStateException("Failed to send command to server");
        }
    }

    @Override
    public void close() throws KvsConnectionException {
        try {
            log.info("Closing socket connection");
            socket.close();
        } catch (IOException e) {
            log.error("Error while closing socket", e);
            throw new KvsConnectionException("Cannot close", e);
        }
    }
}
