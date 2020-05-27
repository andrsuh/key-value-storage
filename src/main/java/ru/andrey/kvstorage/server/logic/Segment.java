package ru.andrey.kvstorage.server.logic;

import ru.andrey.kvstorage.server.exception.DatabaseException;

import java.io.IOException;

public interface Segment {

    /**
     * Возвращает имя сегмента.
     *
     * @return имя сегмента
     */
    String getName();

    /**
     * Записывает значение по указанному ключу в сегмент.
     *
     * @param objectKey ключ, по которому нужно записать значение
     * @param objectValue значение, которое нужно записать
     * @return {@code true} - если значение записалось, {@code false} - если нет
     * @throws IOException если произошла ошибка ввода-вывода.
     * @throws DatabaseException
     */
    // todo sukhoa in future may return something like SegmentWriteResult .. with report and error details?
    // for new returns false if cannot allocate requested capacity
    // exception is questionable
    boolean write(String objectKey, String objectValue) throws IOException, DatabaseException;

    /**
     * Считывает значение из сегмента по переданному ключу.
     *
     * @param objectKey ключ, по которому нужно получить значение
     * @return значение, которое находится по ключу
     * @throws IOException если произошла ошибка ввода-вывода
     */
    String read(String objectKey) throws IOException;

    /**
     * Возвращает {@code true} - если данный сегмент открыт только на чтение, {@code false} - если данный сегмент открыт на чтение и запись.
     *
     * @return {@code true} - если данный сегмент открыт только на чтение, {@code false} - если данный сегмент открыт на чтение и запись
     */
    boolean isReadOnly();
}