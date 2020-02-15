package ru.andrey.kvstorage.logic.impl;

import ru.andrey.kvstorage.DatabaseException;
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
    private final String segmentName;
    private final Path segmentPath;
    private final int currentSize = 0;
    private final Index segmentIndex = new SegmentIndex(); // todo sukhoa this of better design

    public SegmentImpl(String segmentName, Path tableRootPath) throws IOException {
        this.segmentName = segmentName;
        this.segmentPath = Files.createFile(tableRootPath.resolve(segmentName));
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

            segmentIndex.update(objectKey, new IndexInfoImpl(startPosition, length));
            System.out.println(" Written :" + length + " bytes, offset: " + startPosition);
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
