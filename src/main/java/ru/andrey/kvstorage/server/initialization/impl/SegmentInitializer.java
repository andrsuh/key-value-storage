package ru.andrey.kvstorage.server.initialization.impl;

import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.SegmentIndex;
import ru.andrey.kvstorage.server.index.impl.SegmentOffsetInfoImpl;
import ru.andrey.kvstorage.server.initialization.InitializationContext;
import ru.andrey.kvstorage.server.initialization.Initializer;
import ru.andrey.kvstorage.server.initialization.SegmentInitializationContext;
import ru.andrey.kvstorage.server.initialization.TableInitializationContext;
import ru.andrey.kvstorage.server.logic.Segment;
import ru.andrey.kvstorage.server.logic.impl.DatabaseInputStream;
import ru.andrey.kvstorage.server.logic.impl.DatabaseRow;
import ru.andrey.kvstorage.server.logic.impl.SegmentImpl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class SegmentInitializer implements Initializer {

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
        int segmentSize = 0;
        try (DatabaseInputStream in = new DatabaseInputStream(
                new BufferedInputStream(Files.newInputStream(segmentContext.getSegmentPath())))) {
            var offset = 0;

            Optional<DatabaseRow> storingUnit = in.readDbUnit();
            while (storingUnit.isPresent()) {
                DatabaseRow unit = storingUnit.get();
                SegmentOffsetInfoImpl segmentIndexInfo = new SegmentOffsetInfoImpl(offset);
                offset += unit.size();

                String keyString = new String(unit.getKey());
                keys.add(keyString);
                index.onIndexedEntityUpdated(keyString, segmentIndexInfo);
                segmentSize = offset;

                storingUnit = in.readDbUnit();
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot read segment: " + segmentContext.getSegmentPath(), e);
        }

        SegmentInitializationContext segmentInitializationContext = SegmentInitializationContextImpl.builder()
                .segmentName(segmentContext.getSegmentName())
                .segmentPath(segmentContext.getSegmentPath())
                .currentSize(segmentSize) // todo sukhoa set readOnly flag!!!! :)
                .index(index)
                .build();

        SegmentImpl segment = new SegmentImpl(segmentInitializationContext);

        tableIndexUpdate(context.currentTableContext(), keys, segment);
        context.currentTableContext().updateCurrentSegment(segment); // as they are initialized sequentially in accordance to creation time we can do this
    }

    private void tableIndexUpdate(TableInitializationContext tableContext, Set<String> keysInSegment, Segment segmentRef) {
        keysInSegment.forEach(k -> tableContext.getTableIndex().onIndexedEntityUpdated(k, segmentRef));
    }

}
