package ru.andrey.kvstorage.server.initialization.impl;

import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.SegmentIndex;
import ru.andrey.kvstorage.server.index.impl.SegmentOffsetInfoImpl;
import ru.andrey.kvstorage.server.initialization.InitializationContext;
import ru.andrey.kvstorage.server.initialization.Initializer;
import ru.andrey.kvstorage.server.initialization.SegmentInitializationContext;
import ru.andrey.kvstorage.server.initialization.TableInitializationContext;
import ru.andrey.kvstorage.server.logic.DatabaseRecord;
import ru.andrey.kvstorage.server.logic.Segment;
import ru.andrey.kvstorage.server.logic.impl.SegmentImpl;
import ru.andrey.kvstorage.server.logic.io.DatabaseInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;


public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        SegmentInitializationContext segmentContext = context.currentSegmentContext();

        System.out.println("Creating segment " + segmentContext.getSegmentName());

        if (!Files.exists(segmentContext.getSegmentPath())) {
            throw new DatabaseException("Segment with such name doesn't exist: " + segmentContext.getSegmentName());
        }

        SegmentIndex index = new SegmentIndex();
        Set<String> keys = new HashSet<>();
        // todo sukhoa we should read all segments sorting by timestamp
        int indexedSegmentSize = 0;
        try (DatabaseInputStream in = new DatabaseInputStream(
                new BufferedInputStream(Files.newInputStream(segmentContext.getSegmentPath())))) {

            Optional<DatabaseRecord> databaseRecordOptional = in.readDbUnit();
            while (databaseRecordOptional.isPresent()) {
                DatabaseRecord databaseRecord = databaseRecordOptional.get();
                String keyString = new String(databaseRecord.getKey());
                keys.add(keyString);
                index.onIndexedEntityUpdated(keyString, new SegmentOffsetInfoImpl(indexedSegmentSize));


                indexedSegmentSize += databaseRecord.size();
                databaseRecordOptional = in.readDbUnit();
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot read segment: " + segmentContext.getSegmentPath(), e);
        }

        SegmentInitializationContext segmentInitializationContext = SegmentInitializationContextImpl.builder()
                .segmentName(segmentContext.getSegmentName())
                .segmentPath(segmentContext.getSegmentPath())
                .currentSize(indexedSegmentSize) // todo sukhoa set readOnly flag!!!! :)
                .index(index)
                .build();

        Segment segment = SegmentImpl.initializeFromContext(segmentInitializationContext);

        tableIndexUpdate(context.currentTableContext(), keys, segment);
        context.currentTableContext().updateCurrentSegment(segment); // as they are initialized sequentially in accordance to creation time we can do this
    }

    private void tableIndexUpdate(TableInitializationContext tableContext, Set<String> keysInSegment, Segment segmentRef) {
        keysInSegment.forEach(k -> tableContext.getTableIndex().onIndexedEntityUpdated(k, segmentRef));
    }

}
