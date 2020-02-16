package ru.andrey.kvstorage.logic.impl;

import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.logic.Segment;
import ru.andrey.kvstorage.logic.Table;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    private Segment currentSegment;
    private final Map<String, Segment> segments = new LinkedHashMap<>();


    private TableImpl(String tableName, Path pathToDatabaseRoot) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(pathToDatabaseRoot);

        this.tableName = tableName;
        this.tablePath = pathToDatabaseRoot.resolve(tableName);
    }

    static Table create(String tableName, Path pathToDatabaseRoot) throws DatabaseException {
        TableImpl tb = new TableImpl(tableName, pathToDatabaseRoot);
        tb.initializeAsNew();
        return tb;
    }

    static Table existing(String tableName, Path pathToDatabaseRoot) throws DatabaseException {
        TableImpl tb = new TableImpl(tableName, pathToDatabaseRoot);
        tb.initializeAsExisting();
        return tb;
    }

    private void initializeAsNew() throws DatabaseException {
        if (Files.exists(tablePath)) { // todo sukhoa race condition
            throw new DatabaseException("Table with such name already exists: " + tableName);
        }

        try {
            Files.createDirectory(tablePath);
            currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), tablePath);
            segments.put(currentSegment.getName(), currentSegment);

        } catch (IOException e) {
            throw new DatabaseException("Cannot create table directory for path: " + tablePath, e);
        }
    }

    private void initializeAsExisting() throws DatabaseException {
        if (!Files.exists(tablePath)) { // todo sukhoa race condition
            throw new DatabaseException("Table with such name doesn't exist: " + tableName);
        }

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(tablePath, p -> Files.isRegularFile(p)); // todo sukhoa make faster by making parallel
             Stream<Path> directoryStream = StreamSupport.stream(ds.spliterator(), false)) {

            directoryStream
                    .forEach(s -> {
                        try {
                            currentSegment = SegmentImpl.existing(s.getFileName().toString(), tablePath); // todo sukhoa handle case with more than one segment
                            segments.put(currentSegment.getName(), currentSegment);
                        } catch (DatabaseException e) {
                            throw new RuntimeException("", e);
                        }
                    });
        } catch (Exception e) { // todo sukhoa handle this. refactor
            throw new DatabaseException(e);
        }
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        try {
            currentSegment.write(objectKey, objectValue);
        } catch (IOException e) {
            throw new DatabaseException(e); // todo sukhoa review exceptions
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

    @Override
    public String getName() {
        return tableName;
    }
}
