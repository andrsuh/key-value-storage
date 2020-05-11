package ru.andrey.kvstorage.server.initialization.impl;

import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.SegmentIndexImpl;
import ru.andrey.kvstorage.server.index.impl.SegmentIndexInfoImpl;
import ru.andrey.kvstorage.server.initialization.InitializationContext;
import ru.andrey.kvstorage.server.initialization.Initializer;
import ru.andrey.kvstorage.server.initialization.SegmentInitializationContext;
import ru.andrey.kvstorage.server.initialization.TableInitializationContext;
import ru.andrey.kvstorage.server.logic.Segment;
import ru.andrey.kvstorage.server.logic.impl.SegmentImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

public class SegmentInitializer implements Initializer {

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        SegmentInitializationContext segmentContext = context.currentSegmentContext();

        System.out.println("Creating segment " + segmentContext.getSegmentName());

        if (!Files.exists(segmentContext.getSegmentPath())) { // todo sukhoa race condition
            throw new DatabaseException("Segment with such name doesn't exist: " + segmentContext.getSegmentName());
        }

        SegmentIndexImpl index = new SegmentIndexImpl();
        Set<String> keys = new HashSet<>();
        // todo sukhoa we should read all segments sorting by timestamp
        int segmentSize = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(segmentContext.getSegmentPath())))) { // todo sukhoa: is it relayable to count bytes this way not using ByteChannel
            var offset = 0;
            String kvPair = reader.readLine();
            while (kvPair != null) {
                String[] split = kvPair.split(":");// todo sukhoa separator
                String key = split[0];
                SegmentIndexInfoImpl segmentIndexInfo = new SegmentIndexInfoImpl(offset, kvPair.length());

                keys.add(key);
                index.onSegmentUpdated(key, segmentIndexInfo);

                offset = offset + kvPair.length() + 1; // + \n
                segmentSize += kvPair.length();
                kvPair = reader.readLine();
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
        keysInSegment.forEach(k -> tableContext.getTableIndex().onTableUpdated(k, segmentRef));
    }

}
