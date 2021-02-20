package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespError;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public interface DatabaseCommandResult extends DatabaseApiSerializable {

    /**
     * Формирует успешный результат выполнения команды из значения результата.
     *
     * @param result значение результата
     * @return успешный результат выполнения команды, который был сформирован
     */
    static DatabaseCommandResult success(byte[] result) {
        return new DatabaseCommandResultImpl(result, null, DatabaseCommandStatus.SUCCESS);
    }

    /**
     * Формирует результат команды, при выполнении которой произошла ошибка.
     *
     * @param message сообщение об ошибке
     * @return результат команды, при выполнении которой произошла ошибка
     */
    static DatabaseCommandResult error(String message) {
        Objects.requireNonNull(message);
        return new DatabaseCommandResultImpl(null, message, DatabaseCommandStatus.FAILED);
    }

    /**
     * Формирует результат команды, при выполнении которой произошла ошибка.
     *
     * @param exception исключение, из которого нужно сформировать результат выполнения команды
     * @return результат команды, при выполнении которой произошла ошибка
     */
    static DatabaseCommandResult error(Exception exception) {
        Objects.requireNonNull(exception);
        String message = exception.getMessage() != null
                ? exception.getMessage()
                : Arrays.toString(exception.getStackTrace());
        return DatabaseCommandResult.error(message);
    }

    /**
     * @return значение результата выполнения команды в виде {@code Optional<String>}
     */
    Optional<byte[]> getResult();

    /**
     * Возвращает статус выполнения команды.
     *
     * @return статус выполнения команды
     */
    DatabaseCommandStatus getStatus();

    /**
     * Возвращает {@code true} - если команда выполнилась успешно (status == DatabaseCommandStatus.SUCCESS), {@code false} - в ином случае.
     *
     * @return {@code true} - если команда выполнилась успешно (status == DatabaseCommandStatus.SUCCESS), {@code false} - в ином случае
     */
    boolean isSuccess();

    /**
     * Возвращает сообщение об ошибке, которая произошла при выполнении команды.
     *
     * @return сообщение об ошибке, которая произошла при выполнении команды
     */
    String getErrorMessage();

    /**
     * Перечисление, описывающее возможные варианты статуса выполнения команды.
     */
    enum DatabaseCommandStatus {
        SUCCESS, FAILED
    }

    class DatabaseCommandResultImpl implements DatabaseCommandResult {
        private final byte[] result;
        private final String errorMessage;
        private final DatabaseCommandStatus status;

        private DatabaseCommandResultImpl(byte[] result, String errorMessage, DatabaseCommandStatus status) {
            this.result = result;
            this.errorMessage = errorMessage;
            this.status = status;
        }

        @Override
        public boolean isSuccess() {
            return status == DatabaseCommandStatus.SUCCESS;
        }

        @Override
        public DatabaseCommandStatus getStatus() {
            return status;
        }

        @Override
        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public Optional<byte[]> getResult() {
            return Optional.ofNullable(result);
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
    }
}