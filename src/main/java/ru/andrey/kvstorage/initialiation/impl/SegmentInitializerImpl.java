package ru.andrey.kvstorage.initialiation.impl;

import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.initialiation.SegmentInitializationContext;
import ru.andrey.kvstorage.initialiation.SegmentInitializer;
import ru.andrey.kvstorage.logic.IndexInfoImpl;
import ru.andrey.kvstorage.logic.SegmentIndex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;

public class SegmentInitializerImpl implements SegmentInitializer {

    @Override
    public void prepareContext(SegmentInitializationContext context) throws DatabaseException {
        System.out.println("Creating segment " + context.getSegmentName());

        if (!Files.exists(context.getSegmentPath())) { // todo sukhoa race condition
            throw new DatabaseException("Segment with such name doesn't exist: " + context.getSegmentName());
        }

        SegmentIndex index = new SegmentIndex();

        // todo sukhoa we should read all segments sorting by timestamp
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(context.getSegmentPath())))) { // todo sukhoa: is it relayable to count bytes this way not using ByteChannel
            var offset = 0;
            String kvPair = reader.readLine();
            while (kvPair != null) {
                String[] split = kvPair.split(":");// todo sukhoa separator
                index.update(split[0], new IndexInfoImpl(offset, kvPair.length()));

                offset = offset + kvPair.length() + 1; // + \n
                kvPair = reader.readLine();
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot read segment: " + context.getSegmentPath(), e);
        }

        context.setSegmentIndex(index);
    }
}
