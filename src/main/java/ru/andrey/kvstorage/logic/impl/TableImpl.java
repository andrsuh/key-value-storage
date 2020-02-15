package ru.andrey.kvstorage.logic.impl;

import ru.andrey.kvstorage.DatabaseException;
import ru.andrey.kvstorage.logic.Segment;
import ru.andrey.kvstorage.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {
    private final String tableName;
    private final Path tablePath;
    private final Segment currentSegment;
    private final Map<String, Segment> segments = new LinkedHashMap<>();


    public TableImpl(String tableName, Path pathToDatabaseRoot) throws IOException {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(pathToDatabaseRoot);

        this.tableName = tableName;
        this.tablePath = Files.createDirectory(pathToDatabaseRoot.resolve(tableName));

        currentSegment = new SegmentImpl(SegmentImpl.createSegmentName(tableName), tablePath);
        segments.put(currentSegment.getName(), currentSegment);
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        try {
            currentSegment.write(objectKey, objectValue);
        } catch (IOException e) {
            throw new DatabaseException(e); // todo sukhoa review
        }
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        try {
            return currentSegment.read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }
}
