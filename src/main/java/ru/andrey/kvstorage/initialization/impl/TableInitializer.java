package ru.andrey.kvstorage.initialization.impl;

import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.initialization.InitializationContext;
import ru.andrey.kvstorage.initialization.Initializer;
import ru.andrey.kvstorage.initialization.TableInitializationContext;
import ru.andrey.kvstorage.logic.impl.TableImpl;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TableInitializer implements Initializer {

    private Initializer segmentInitializer;

    public TableInitializer(Initializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        TableInitializationContext tableContext = context.currentTableContext();

        System.out.println("Creating table " + tableContext.getTableName());

        if (!Files.exists(tableContext.getTablePath())) { // todo sukhoa race condition
            throw new DatabaseException("Table with such name doesn't exist: " + tableContext.getTableName());
        }

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(tableContext.getTablePath(), p -> Files.isRegularFile(p)); // todo sukhoa make faster by making parallel
             Stream<Path> directoryStream = StreamSupport.stream(ds.spliterator(), false)) {

            directoryStream
                    .sorted((f1, f2) -> {
                        long time1 = segmentCreationTimeByName(f1.getFileName().toString());
                        long time2 = segmentCreationTimeByName(f2.getFileName().toString());

                        return Long.compare(time1, time2);
                    })
                    .forEach(s -> {
                        String segmentName = s.getFileName().toString();

                        InitializationContext downstreamContext = InitializationContextImpl.builder()
                                .executionEnvironment(context.executionEnvironment())
                                .currentDatabaseContext(context.currentDbContext())
                                .currentTableContext(context.currentTableContext())
                                .currentSegmentContext(new SegmentInitializationContextImpl(segmentName, tableContext.getTablePath(), 0)) // todo sukhao fix size
                                .build();
                        try {
                            segmentInitializer.perform(downstreamContext);
                        } catch (DatabaseException e) { // todo sukhoa make them throw unchecked exception
                            throw new RuntimeException(e);
                        }

                        context.currentDbContext().addTable(new TableImpl(context.currentTableContext()));
                    });
        } catch (Exception e) { // todo sukhoa handle this. refactor
            throw new DatabaseException(e);
        }
    }

    private long segmentCreationTimeByName(String name) {
        return Arrays.stream(name.split("_"))
                .skip(2)
                .map(Long::valueOf)
                .findFirst().orElseThrow(() -> new IllegalStateException("No datatime provided for segment name :" + name));
    }
}
