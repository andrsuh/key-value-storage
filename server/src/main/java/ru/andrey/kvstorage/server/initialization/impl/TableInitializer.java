package ru.andrey.kvstorage.server.initialization.impl;

import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.initialization.InitializationContext;
import ru.andrey.kvstorage.server.initialization.Initializer;
import ru.andrey.kvstorage.server.initialization.TableInitializationContext;
import ru.andrey.kvstorage.server.logic.Table;
import ru.andrey.kvstorage.server.logic.impl.TableImpl;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TableInitializer implements Initializer {

    private final SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        TableInitializationContext tableContext = context.currentTableContext();

        System.out.println("Creating table " + tableContext.getTableName());

        if (!Files.exists(tableContext.getTablePath())) {
            throw new DatabaseException("Table with such name doesn't exist: " + tableContext.getTableName());
        }

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(tableContext.getTablePath(), Files::isRegularFile); // todo sukhoa make faster by making parallel
            Stream<Path> directoryStream = StreamSupport.stream(ds.spliterator(), false)) {
            directoryStream
                    .sorted(this::compareSegments)
                    .forEach(path -> initializeSegment(context, path));
            Table initializedTable = TableImpl.initializeFromContext(context.currentTableContext());
            context.currentDbContext().addTable(initializedTable);
        } catch (Exception e) { // todo sukhoa handle this. refactor
            throw new DatabaseException(e);
        }
    }

    private void initializeSegment(InitializationContext context, Path segmentPath) {
        TableInitializationContext tableContext = context.currentTableContext();
        String segmentName = segmentPath.getFileName().toString();

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
    }

    private int compareSegments(Path segment1, Path segment2) {
        long time1 = segmentCreationTimeByName(segment1.getFileName().toString());
        long time2 = segmentCreationTimeByName(segment2.getFileName().toString());
        return Long.compare(time1, time2);
    }

    private long segmentCreationTimeByName(String name) {
        return Arrays.stream(name.split("_"))
                .skip(1)
                .map(Long::valueOf)
                .findFirst().orElseThrow(() -> new IllegalStateException("No datatime provided for segment name: " + name));
    }
}
