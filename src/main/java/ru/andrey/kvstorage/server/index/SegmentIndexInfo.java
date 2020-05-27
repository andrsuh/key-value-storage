package ru.andrey.kvstorage.server.index;

public interface SegmentIndexInfo {

    /**
     * Возвращает смещение определенной записи в файле сегмента.
     *
     * @return смещение определенной записи в файле сегмента
     */
    long getOffset();
}
