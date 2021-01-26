package ru.andrey.kvstorage.resp;

import lombok.Getter;

@Getter
public enum CommandByte {
    CR((byte) '\r'),
    LF((byte) '\n'),
    SIMPLE_STRING_IDENTIFIER((byte) '+'),
    MINUS((byte) '-'),
    BULK_STRING_IDENTIFIER((byte) '$'),
    ARRAY_IDENTIFIER((byte) '*'),
    COMMAND_ID((byte) '!');

    private final byte symbolByte;

    CommandByte(byte symbol) {
        this.symbolByte = symbol;
    }

    public static CommandByte getFromValue(byte value) {
        for (CommandByte commandByte : CommandByte.values()) {
            if (commandByte.symbolByte == value) {
                return commandByte;
            }
        }
        throw new IllegalArgumentException("Unknown type character");
    }
}
