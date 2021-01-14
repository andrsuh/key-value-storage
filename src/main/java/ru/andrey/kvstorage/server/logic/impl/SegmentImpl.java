package ru.andrey.kvstorage.server.logic.impl;

import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.index.SegmentOffsetInfo;
import ru.andrey.kvstorage.server.index.impl.SegmentIndex;
import ru.andrey.kvstorage.server.index.impl.SegmentOffsetInfoImpl;
import ru.andrey.kvstorage.server.initialization.SegmentInitializationContext;
import ru.andrey.kvstorage.server.logic.Segment;
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
 * - имеет ограниченный размер
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {
    private static final long MAX_SEGMENT_SIZE = 100_000L; // todo sukhoa use properties

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

    public SegmentImpl(SegmentInitializationContext context) {
        this.readOnly = true;
        this.segmentName = context.getSegmentName();
        this.segmentPath = context.getSegmentPath();
        this.segmentIndex = context.getIndex();
        this.currentSizeInBytes = context.getCurrentSize();
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        SegmentImpl sg = new SegmentImpl(segmentName, tableRootPath);
        sg.initializeAsNew();
        return sg;
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
        // todo sukhoa remove this shit after min allowed segment size is set
        //        try {
        //            Thread.sleep(1);
        //        } catch (InterruptedException e) {
        //            e.printStackTrace();
        //        }
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, String objectValue) throws IOException { // todo sukhoa deal with second exception

        DatabaseRow storingUnit = new DatabaseRow(objectKey, objectValue);

        if (!canAllocate(storingUnit.size())) {
            System.out.println("Segment " + segmentName + " is full. Current size : " + currentSizeInBytes);
            readOnly = true;
            return false;
        }

        currentSizeInBytes = currentSizeInBytes + storingUnit.size();

        try (SeekableByteChannel byteChannel = Files.newByteChannel(segmentPath, StandardOpenOption.APPEND);
             DatabaseOutputStream out = new DatabaseOutputStream(Channels.newOutputStream(byteChannel))) {

            var startPosition = byteChannel.position();
            out.write(storingUnit);

            segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(startPosition));
        }
        return true; // todo sukhoa fix
    }

    private boolean canAllocate(long length) {
        return MAX_SEGMENT_SIZE - currentSizeInBytes >= length;
    }

    @Override
    public String read(String objectKey) throws IOException {
        Optional<SegmentOffsetInfo> indexInfo = segmentIndex.searchForKey(objectKey);

        if (indexInfo.isEmpty()) {
            throw new IllegalStateException("Read nonexistent key");
        }

        try (SeekableByteChannel byteChannel = Files.newByteChannel(segmentPath, StandardOpenOption.READ);
             DatabaseInputStream in = new DatabaseInputStream(Channels.newInputStream(byteChannel))) {

            byteChannel.position(indexInfo.get().getOffset());

            DatabaseRow unit = in.readDbUnit().orElseThrow(() -> new IllegalStateException("Not enough bytes"));

            return new String(unit.getValue()); // todo sukhoa charset, handle separator
        }
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }
}
