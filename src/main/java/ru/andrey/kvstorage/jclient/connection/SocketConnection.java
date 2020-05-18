package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.jclient.exception.KvsConnectionException;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Arrays;

public class SocketConnection implements KvsConnection {

    private final Socket socket;

    public SocketConnection() {
        try {
            this.socket = new Socket("127.0.0.1", 4321);// todo sukhoa remove magic constant
        } catch (IOException e) {
            throw new IllegalStateException("Cannot connect", e);
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

    @Override
    public byte[] send(byte[] command) {
        if (socket.isClosed()) {
            throw new IllegalStateException("Socket is closed");
        }
        try {
            socket.getOutputStream().write(command);
            InputStream inputStream = socket.getInputStream();
            int b, i = 0;
            byte[] res = new byte[1000]; // todo sukhoa remove magic constant
            while ((b = inputStream.read()) != -1 && b != '\r') {
                res[i++] = (byte) b;
            }
            return Arrays.copyOfRange(res, 0, i);
        } catch (IOException e) {
            throw new IllegalStateException("obosralsa");
        }
    }
}
