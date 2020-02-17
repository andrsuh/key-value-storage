package ru.andrey.kvstorage.logic.impl;

import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.initialiation.SegmentInitializationContext;
import ru.andrey.kvstorage.logic.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    private static final long MAX_SEGMENT_SIZE = 20;

    private final String segmentName;
    private final Path segmentPath;
    private final Index segmentIndex; // todo sukhoa think of better design

    private long currentSizeInBytes;
    private volatile boolean readOnly = true;

    private SegmentImpl(String segmentName, Path tableRootPath) {
        this.segmentName = segmentName;
        this.segmentPath = tableRootPath.resolve(segmentName);
        this.segmentIndex = new SegmentIndex();
        this.currentSizeInBytes = 0;
        this.readOnly = false;
    }

    public SegmentImpl(SegmentInitializationContext context) {
        this.segmentName = context.getSegmentName();
        this.segmentPath = context.getSegmentPath();
        this.segmentIndex = context.getSegmentIndex();
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
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, String objectValue) throws IOException, DatabaseException { // todo sukhoa deal with second exception
        byte[] content = (objectKey + ":" + objectValue + "\n").getBytes();// todo sukhoa add charset,  create separator field (property!)
        int length = content.length;

        if (!canAllocate(length)) { // todo sukhoa race condition
            System.out.println("Segment " + segmentName + " is full. Current size : " + currentSizeInBytes);
            readOnly = false;
            return false;
        }

        currentSizeInBytes = currentSizeInBytes + length;

        try (SeekableByteChannel byteChannel = Files.newByteChannel(segmentPath, StandardOpenOption.APPEND);
             OutputStream out = Channels.newOutputStream(byteChannel)) {

            var startPosition = byteChannel.position();
            out.write(content);

            segmentIndex.update(objectKey, new IndexInfoImpl(startPosition, length - 1)); // excluding \n
        }
        return true; // todo sukhoa fix
    }

    private boolean canAllocate(int length) {
        return MAX_SEGMENT_SIZE - currentSizeInBytes >= length;
    }

    @Override
    public String read(String objectKey) throws IOException {
        Optional<IndexInfo> indexInfo = segmentIndex.searchForKey(objectKey);

        if (indexInfo.isEmpty()) {
            throw new IllegalStateException("Read nonexistent key");
        }

        try (SeekableByteChannel byteChannel = Files.newByteChannel(segmentPath, StandardOpenOption.READ);
             InputStream in = Channels.newInputStream(byteChannel)) {

            byteChannel.position(indexInfo.get().getOffset());
            byte[] bytes = in.readNBytes((int) indexInfo.get().getLength()); // todo sukhoa handle this typecast

            return new String(bytes).split(":")[1]; // todo sukhoa charset, handle separator
        }
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }
}
