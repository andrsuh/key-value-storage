package ru.andrey.kvstorage.jclient;

public class DatabaseResponseParser {
    private static final String SEPARATOR = "\r\n";
    private static final char START_BYTE = '#';
    private static final char STRING_BYTE = '$';
    private static final char ERROR_BYTE = '-';

    public String parseResponse(byte[] response) {
        if (response[0] != START_BYTE) {
            throw new IllegalArgumentException("#");
        }

        StringBuilder result = new StringBuilder();
        int i = 2;
        while (response[i] != '\r') {
            result.append((char) response[i++]);
        }

        switch (response[1]) {
            case ERROR_BYTE:
                throw new RuntimeException(result.toString());
            case STRING_BYTE:
                return result.toString();
            default:
                throw new IllegalStateException("Wrong byte : " + response[1]);
        }
    }
}
