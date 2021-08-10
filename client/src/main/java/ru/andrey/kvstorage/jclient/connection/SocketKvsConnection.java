package ru.andrey.kvstorage.jclient.connection;

import ru.andrey.kvstorage.jclient.exception.ConnectionException;
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
    /**
     * Читает из инпут стрима сокета. Используется для чтения ответа
     */
    private final RespReader reader;
    /**
     * Пишет в аутпут стрим сокета. Используется для отправки команд
     */
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

    /**
     * Отправляет с помощью сокета команду и получает результат.
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        if (socket.isClosed()) {
            throw new ConnectionException("Socket is closed");
        }

        try {
            writer.write(command);
            RespObject result = reader.readObject();
            System.out.println("Client got response to command id: " + commandId);
            return result;
        } catch (IOException e) {
            throw new ConnectionException("Failed to send command to server", e);
        }
    }

    /**
     * Закрывает сокет
     */
    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot close", e);
        }
    }
}
