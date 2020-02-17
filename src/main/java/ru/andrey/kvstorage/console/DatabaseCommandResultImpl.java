package ru.andrey.kvstorage.console;

public class DatabaseCommandResultImpl implements DatabaseCommandResult {
    private final String message;
    private final String result;

    public DatabaseCommandResultImpl(String message, String result) {
        this.message = message;
        this.result = result;
    }

    @Override
    public String getResultMessage() {
        return message;
    }

    @Override
    public String getResult() {
        return result;
    }
}
