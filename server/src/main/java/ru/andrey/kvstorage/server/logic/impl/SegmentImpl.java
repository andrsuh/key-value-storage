package ru.andrey.kvstorage.server.logic.impl;

import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.index.SegmentOffsetInfo;
import ru.andrey.kvstorage.server.index.impl.SegmentIndex;
import ru.andrey.kvstorage.server.index.impl.SegmentOffsetInfoImpl;
import ru.andrey.kvstorage.server.initialization.SegmentInitializationContext;
import ru.andrey.kvstorage.server.logic.DatabaseRecord;
import ru.andrey.kvstorage.server.logic.Segment;
import ru.andrey.kvstorage.server.logic.WritableDatabaseRecord;
import ru.andrey.kvstorage.server.logic.io.DatabaseInputStream;
import ru.andrey.kvstorage.server.logic.io.DatabaseOutputStream;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер, большие значения (>100000) записываются в последний сегмент, если он не read-only
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {
    private static final long MAX_SEGMENT_SIZE = 100_000; // todo sukhoa use properties

    private final String segmentName;
    private final Path segmentPath;
    private final SegmentIndex segmentIndex;

    private long currentSizeInBytes;
    private volatile boolean readOnly = false;

    private SegmentImpl(String segmentName, Path tableRootPath) {
        this.segmentName = segmentName;
        this.segmentPath = tableRootPath.resolve(segmentName);
        this.segmentIndex = new SegmentIndex();
        this.currentSizeInBytes = 0;
    }

    private SegmentImpl(SegmentInitializationContext context) {
        this.readOnly = context.getCurrentSize() >= MAX_SEGMENT_SIZE;
        this.segmentName = context.getSegmentName();
        this.segmentPath = context.getSegmentPath();
        this.segmentIndex = context.getIndex();
        this.currentSizeInBytes = context.getCurrentSize();
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        SegmentImpl sg = new SegmentImpl(segmentName, tableRootPath);
        sg.initializeAsNew();
        return sg;
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        return new SegmentImpl(context);
    }

    private void initializeAsNew() throws DatabaseException {
        if (Files.exists(segmentPath)) { // todo sukhoa race condition
            throw new DatabaseException("Segment with such name already exists: " + segmentName);
        }

        try {
            Files.createFile(segmentPath);
        } catch (IOException e) {
            throw new DatabaseException("Cannot create segment file for path: " + segmentPath, e);
        }
    }

    static String createSegmentName(String tableName) {
        try {
            Thread.sleep(1);
        } catch (InterruptedException ignored) {}
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        WritableDatabaseRecord databaseRecord = new SetDatabaseRecord(objectKey, objectValue);
        return updateSegment(databaseRecord);
    }

    private boolean canAllocate() {
        return currentSizeInBytes < MAX_SEGMENT_SIZE;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        Optional<SegmentOffsetInfo> indexInfo = segmentIndex.searchForKey(objectKey);

        if (indexInfo.isEmpty()) {
            return Optional.empty();
        }

        try (SeekableByteChannel byteChannel = Files.newByteChannel(segmentPath, StandardOpenOption.READ);
             DatabaseInputStream in = new DatabaseInputStream(Channels.newInputStream(byteChannel))) {

            byteChannel.position(indexInfo.get().getOffset());

            DatabaseRecord record = in.readDbUnit().orElseThrow(() -> new IllegalStateException("Not enough bytes"));

            return Optional.ofNullable(record.isValuePresented() ? record.getValue() : null);
        }
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        WritableDatabaseRecord emptyRow = new RemoveDatabaseRecord(objectKey);
        return updateSegment(emptyRow);
    }

    private boolean updateSegment(WritableDatabaseRecord databaseRecord) throws IOException {
        if (!canAllocate()) {
            System.out.println("Segment " + segmentName + " is full. Current size : " + currentSizeInBytes);
            readOnly = true;
            return false;
        }

        currentSizeInBytes = currentSizeInBytes + databaseRecord.size();

        try (SeekableByteChannel byteChannel = Files.newByteChannel(segmentPath, StandardOpenOption.APPEND);
             DatabaseOutputStream out = new DatabaseOutputStream(Channels.newOutputStream(byteChannel))) {

            var startPosition = byteChannel.position();
            out.write(databaseRecord);

            segmentIndex.onIndexedEntityUpdated(new String(databaseRecord.getKey()), new SegmentOffsetInfoImpl(startPosition));
        }
        return true;
    }
}
