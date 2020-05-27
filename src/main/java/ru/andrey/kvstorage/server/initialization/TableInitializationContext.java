package ru.andrey.kvstorage.server.initialization;

import ru.andrey.kvstorage.server.index.impl.TableIndex;
import ru.andrey.kvstorage.server.logic.Segment;

import java.nio.file.Path;

public interface TableInitializationContext {

    /**
     * Возвращает имя инициализируемой таблицы.
     *
     * @return имя инициализируемой таблицы
     */
    String getTableName();

    /**
     * Возвращает путь до директории таблицы.
     *
     * @return путь до директории таблицы
     */
    Path getTablePath();

    /**
     * Возвращает индекс инициализируемой таблицы.
     *
     * @return индекс инициализируемой таблицы
     */
    TableIndex getTableIndex();

    /**
     * Возвращает текущий активный сегмент для инициализируемой таблицы.
     *
     * @return текущий активный сегмент для инициализируемой таблицы
     */
    Segment getCurrentSegment();

    /**
     * Обновляет текущий активный сегмент.
     *
     * @param segment новый сегмент
     */
    void updateCurrentSegment(Segment segment); // todo sukhoa refactor?
}
