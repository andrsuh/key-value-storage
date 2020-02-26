package ru.andrey.kvstorage.initialization.impl;

import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.index.SegmentIndexInfo;
import ru.andrey.kvstorage.index.impl.SegmentIndex;
import ru.andrey.kvstorage.index.impl.SegmentIndexInfoImpl;
import ru.andrey.kvstorage.initialization.InitializationContext;
import ru.andrey.kvstorage.initialization.Initializer;
import ru.andrey.kvstorage.initialization.SegmentInitializationContext;
import ru.andrey.kvstorage.initialization.TableInitializationContext;
import ru.andrey.kvstorage.logic.Segment;
import ru.andrey.kvstorage.logic.impl.SegmentImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

public class SegmentInitializer implements Initializer {
    private static final int ENDLINE_SYMBOL_LENGTH = System.getProperty("line.separator").length();
    private static final String KEY_VALUE_SEPARATOR = ":";

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        SegmentInitializationContext segmentContext = context.currentSegmentContext();

        System.out.println("Creating segment " + segmentContext.getSegmentName());

        if (!Files.exists(segmentContext.getSegmentPath())) { // todo sukhoa race condition
            throw new DatabaseException("Segment with such name doesn't exist: " + segmentContext.getSegmentName());
        }

        SegmentIndex<String, SegmentIndexInfo> index = new SegmentIndex<>();
        Set<String> keys = new HashSet<>();
        // todo sukhoa we should read all segments sorting by timestamp
        int segmentSize = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(segmentContext.getSegmentPath())))) { // todo sukhoa: is it relayable to count bytes this way not using ByteChannel
            var offset = 0;
            String kvPair = reader.readLine();
            while (kvPair != null) {
                // TODO A: non-unique key occurrence policy: always pick latest and erase previous?
                String[] split = kvPair.split(KEY_VALUE_SEPARATOR);// todo sukhoa separator A: I think this seprator should not be user defined and should be always a colon (moved to constant)
                String key = split[0];
                SegmentIndexInfoImpl segmentIndexInfo = new SegmentIndexInfoImpl(offset, kvPair.length());

                keys.add(key);
                index.updateIndex(key, segmentIndexInfo);

                offset = offset + kvPair.length() + ENDLINE_SYMBOL_LENGTH;
                segmentSize += kvPair.length(); // TODO A: shouldn't segment include endlines? I think it should
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
        keysInSegment.forEach(k -> {
            tableContext.getTableIndex().updateIndex(k, segmentRef);
        });
    }

}
