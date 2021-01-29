package ru.andrey.kvstorage.server.logic.impl;

import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.TableIndex;
import ru.andrey.kvstorage.server.initialization.TableInitializationContext;
import ru.andrey.kvstorage.server.logic.Segment;
import ru.andrey.kvstorage.server.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

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
    private final TableIndex tableIndex;
    private Segment currentSegment; // todo sukhoa potentially AtomicReference

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(pathToDatabaseRoot);

        this.tableName = tableName;
        this.tablePath = pathToDatabaseRoot.resolve(tableName);

        this.tableIndex = tableIndex;
    }

    private TableImpl(TableInitializationContext context) {
        this.tableName = context.getTableName();
        this.tablePath = context.getTablePath();
        this.tableIndex = context.getTableIndex();
        this.currentSegment = context.getCurrentSegment();
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        TableImpl tb = new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
        tb.initializeAsNew();
        return new CachingTable(tb);
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return new CachingTable(new TableImpl(context));
    }

    private void initializeAsNew() throws DatabaseException {
        if (Files.exists(tablePath)) { // todo sukhoa race condition
            throw new DatabaseException("Table with such name already exists: " + tableName);
        }

        try {
            Files.createDirectory(tablePath);
            currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), tablePath);
        } catch (IOException e) {
            throw new DatabaseException("Cannot create table directory for path: " + tablePath, e);
        }
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        try {
            while (true) { // todo sukhoa
                var s = currentSegment; // cache to local for preventing concurrent issues in future
                if (!s.isReadOnly() && s.write(objectKey, objectValue)) {
                    tableIndex.onIndexedEntityUpdated(objectKey, s);
                    break;
                }
                // todo sukhoa use Atomic reference in future
                currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), tablePath);
            }
        } catch (IOException e) {
            throw new DatabaseException(e); // todo sukhoa review exceptions
        }
    }

    @Override
    public Optional<String> read(String objectKey) throws DatabaseException {
        try {
            Optional<Segment> segment = tableIndex.searchForKey(objectKey);
            if (segment.isEmpty()) {
                return Optional.empty();
            }
            return segment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        try {
            while (true) { // todo sukhoa
                var s = currentSegment; // cache to local for preventing concurrent issues in future
                if (!s.isReadOnly() && s.delete(objectKey)) {
                    tableIndex.onIndexedEntityUpdated(objectKey, s);
                    break;
                }
                // todo sukhoa use Atomic reference in future
                currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), tablePath);
            }
        } catch (IOException e) {
            throw new DatabaseException(e); // todo sukhoa review exceptions
        }
    }

    @Override
    public String getName() {
        return tableName;
    }
}
