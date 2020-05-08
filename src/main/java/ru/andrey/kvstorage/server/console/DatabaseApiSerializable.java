package ru.andrey.kvstorage.server.console;

public interface DatabaseApiSerializable {
    String SEPARATOR = "\r\n";
    String START_BYTE = "#"; // todo sukhoa Byte? or String? :)
    String STRING_BYTE = "$";
    String ERROR_BYTE = "-";

    byte[] toApiBytes();
}
