package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespError;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public class DatabaseCommandResult implements DatabaseApiSerializable {

    /**
     * Формирует успешный результат выполнения команды из значения результата.
     *
     * @param result значение результата
     * @return успешный результат выполнения команды, который был сформирован
     */
    public static DatabaseCommandResult success(byte[] result) {
        return new DatabaseCommandResult(result, null, DatabaseCommandStatus.SUCCESS);
    }

    /**
     * Формирует результат команды, при выполнении которой произошла ошибка.
     *
     * @param message сообщение об ошибке
     * @return результат команды, при выполнении которой произошла ошибка
     */
    public static DatabaseCommandResult error(String message) {
        Objects.requireNonNull(message);
        return new DatabaseCommandResult(null, message, DatabaseCommandStatus.FAILED);
    }

    /**
     * Формирует результат команды, при выполнении которой произошла ошибка.
     *
     * @param exception исключение, из которого нужно сформировать результат выполнения команды
     * @return результат команды, при выполнении которой произошла ошибка
     */
    public static DatabaseCommandResult error(Exception exception) {
        Objects.requireNonNull(exception);
        String message = exception.getMessage() != null
                ? exception.getMessage()
                : Arrays.toString(exception.getStackTrace());
        return DatabaseCommandResult.error(message);
    }

    private final byte[] result;
    private final String errorMessage;
    private final DatabaseCommandStatus status;

    private DatabaseCommandResult(byte[] result, String errorMessage, DatabaseCommandStatus status) {
        this.result = result;
        this.errorMessage = errorMessage;
        this.status = status;
    }

    /**
     * @return значение результата выполнения команды в виде {@code Optional<String>}
     */
    public Optional<byte[]> getResult() {
        return Optional.ofNullable(result);
    }

    /**
     * Возвращает статус выполнения команды.
     *
     * @return статус выполнения команды
     */
    public DatabaseCommandStatus getStatus() {
        return status;
    }

    /**
     * Возвращает {@code true} - если команда выполнилась успешно (status == DatabaseCommandStatus.SUCCESS), {@code false} - в ином случае.
     *
     * @return {@code true} - если команда выполнилась успешно (status == DatabaseCommandStatus.SUCCESS), {@code false} - в ином случае
     */
    public boolean isSuccess() {
        return status == DatabaseCommandStatus.SUCCESS;
    }

    /**
     * Возвращает сообщение об ошибке, которая произошла при выполнении команды.
     *
     * @return сообщение об ошибке, которая произошла при выполнении команды
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public RespObject serialize() {
        if (isSuccess()) {
            if (getResult().isPresent()) {
                return new RespBulkString(result);
            }
            return RespBulkString.NULL_BULK_STRING;
        } else {
            return new RespError(errorMessage.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Перечисление, описывающее возможные варианты статуса выполнения команды.
     */
    public enum DatabaseCommandStatus {
        SUCCESS, FAILED
    }
}