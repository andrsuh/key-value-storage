package ru.andrey.kvstorage.logic.impl;

import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.logic.*;

import java.io.*;
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
    private final String segmentName;
    private final Path segmentPath;
    private final int currentSize = 0;
    private final Index segmentIndex = new SegmentIndex(); // todo sukhoa this of better design

    private SegmentImpl(String segmentName, Path tableRootPath) {
        this.segmentName = segmentName;
        this.segmentPath = tableRootPath.resolve(segmentName);
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        SegmentImpl sg = new SegmentImpl(segmentName, tableRootPath);
        sg.initializeAsNew();
        return sg;
    }

    static Segment existing(String segmentName, Path tableRootPath) throws DatabaseException {
        SegmentImpl sg = new SegmentImpl(segmentName, tableRootPath);
        sg.initializeAsExisting();
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

    private void initializeAsExisting() throws DatabaseException {
        if (!Files.exists(segmentPath)) { // todo sukhoa race condition
            throw new DatabaseException("Segment with such name doesn't exist: " + segmentName);
        }


        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(segmentPath)))) { // todo sukhoa: is it relayable to count bytes this way not using ByteChannel

            var offset = 0;
            String kvPair = reader.readLine();
            while (kvPair != null) {
                String[] split = kvPair.split(":");// todo sukhoa separator
                segmentIndex.update(split[0], new IndexInfoImpl(offset, kvPair.length()));

                offset = offset + kvPair.length() + 1; // + \n
                kvPair = reader.readLine();
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot read segment: " + segmentPath, e);
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
    public int write(String objectKey, String objectValue) throws IOException, DatabaseException { // todo sukhoa deal with second exception
        byte[] content = (objectKey + ":" + objectValue + "\n").getBytes();// todo sukhoa add charset,  create separator field (property!)
        int length = content.length;

        try (SeekableByteChannel byteChannel = Files.newByteChannel(segmentPath, StandardOpenOption.APPEND);
             OutputStream out = Channels.newOutputStream(byteChannel)) {

            var startPosition = byteChannel.position();
            out.write(content);

            segmentIndex.update(objectKey, new IndexInfoImpl(startPosition, length - 1)); // excluding \n
//            System.out.println(" Written :" + length + " bytes, offset: " + startPosition);
        }


        return 0; // todo sukhoa fix
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
}
