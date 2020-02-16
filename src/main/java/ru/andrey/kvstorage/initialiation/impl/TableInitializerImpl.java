package ru.andrey.kvstorage.initialiation.impl;

import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.initialiation.SegmentInitializer;
import ru.andrey.kvstorage.initialiation.TableInitializationContext;
import ru.andrey.kvstorage.initialiation.TableInitializer;
import ru.andrey.kvstorage.logic.Segment;
import ru.andrey.kvstorage.logic.impl.SegmentImpl;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TableInitializerImpl implements TableInitializer {

    private SegmentInitializer segmentInitializer;

    public TableInitializerImpl(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    @Override
    public void prepareContext(TableInitializationContext context) throws DatabaseException {
        System.out.println("Creating table " + context.getTableName());

        if (!Files.exists(context.getTablePath())) { // todo sukhoa race condition
            throw new DatabaseException("Table with such name doesn't exist: " + context.getTableName());
        }

        Map<String, Segment> segments = new HashMap<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(context.getTablePath(), p -> Files.isRegularFile(p)); // todo sukhoa make faster by making parallel
             Stream<Path> directoryStream = StreamSupport.stream(ds.spliterator(), false)) {

            directoryStream
                    .forEach(s -> {
                        String segmentName = s.getFileName().toString();

                        SegmentInitializationContextImpl segmentInitContext = new SegmentInitializationContextImpl(segmentName, context.getTablePath(), 0);// todo sukhao fix size
                        try {
                            segmentInitializer.prepareContext(segmentInitContext);
                        } catch (DatabaseException e) {
                            throw new RuntimeException(e);
                        }

                        SegmentImpl segment = new SegmentImpl(segmentInitContext); // todo sukhoa handle case with more than one segment
                        segments.put(segment.getName(), segment);
                        context.setCurrentSegment(segment);
                    });
        } catch (Exception e) { // todo sukhoa handle this. refactor
            throw new DatabaseException(e);
        }

        context.setSegments(segments);
    }
}
