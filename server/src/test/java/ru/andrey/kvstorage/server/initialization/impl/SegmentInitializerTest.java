package ru.andrey.kvstorage.server.initialization.impl;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.SegmentIndex;
import ru.andrey.kvstorage.server.index.impl.TableIndex;
import ru.andrey.kvstorage.server.logic.Segment;
import ru.andrey.kvstorage.server.logic.WritableDatabaseRecord;
import ru.andrey.kvstorage.server.logic.impl.SegmentImpl;
import ru.andrey.kvstorage.server.logic.io.DatabaseOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockStatic;

public class SegmentInitializerTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final MockedStatic<SegmentImpl> segmentMockedStatic = mockStatic(SegmentImpl.class);
    private final SegmentInitializer segmentInitializer = new SegmentInitializer();

    @AfterClass
    public static void down() {
        segmentMockedStatic.close();
    }


    @Test(expected = DatabaseException.class)
    public void invalidSegmentInContext_exceptionIsThrown() throws DatabaseException {
            Path path = Paths.get(temporaryFolder.getRoot().getPath(), "segment");
            segmentInitializer.perform(new InitializationContextImpl(null, null, null,
                    new SegmentInitializationContextImpl("segment", path, 0)));
    }

    @Test
    public void validSegmentInContext_segmentIsCreated_tableIndexUpdated() throws DatabaseException, IOException {
        String currentSegment = "current";
        File testFile = new File(temporaryFolder.newFolder("test_table"), currentSegment);
        Path path = testFile.toPath();
        try (DatabaseOutputStream out = new DatabaseOutputStream(new FileOutputStream(testFile))) {
            out.write(new TestRecord("key".getBytes(StandardCharsets.UTF_8), 3, "value".getBytes(StandardCharsets.UTF_8), 5, 16, true));
            out.write(new TestRecord("key".getBytes(StandardCharsets.UTF_8), 3, "value2".getBytes(StandardCharsets.UTF_8), 6, 17, true));
            out.write(new TestRecord("key".getBytes(StandardCharsets.UTF_8), 3, null, -1, 11, false));
            out.write(new TestRecord("key1".getBytes(StandardCharsets.UTF_8), 4, "value1".getBytes(StandardCharsets.UTF_8), 6, 18, true));
            out.write(new TestRecord("key2".getBytes(StandardCharsets.UTF_8), 4, "".getBytes(StandardCharsets.UTF_8), 0, 12, true));
        }

        ArgumentCaptor<SegmentInitializationContextImpl> captor = ArgumentCaptor.forClass(SegmentInitializationContextImpl.class);
        segmentMockedStatic.when(() -> SegmentImpl.initializeFromContext(captor.capture())).thenReturn(new SegmentStub(currentSegment));

        SegmentInitializationContextImpl segmentContext = new SegmentInitializationContextImpl(currentSegment, path, 0, new SegmentIndex());
        TableIndex tableIndex = new TableIndex();
        tableIndex.onIndexedEntityUpdated("key0", new SegmentStub("previous"));
        tableIndex.onIndexedEntityUpdated("key", new SegmentStub("previous"));
        TableInitializationContextImpl tableContext = new TableInitializationContextImpl("table", temporaryFolder.getRoot().toPath(), tableIndex);
        InitializationContextImpl context = new InitializationContextImpl(null, null, tableContext, segmentContext);

        //act
        segmentInitializer.perform(context);

        //assert that the correct context was used to create an index
        SegmentInitializationContextImpl capturedSegmentContext = captor.getValue();
        assertThat("Segment was created from context with incorrect name", capturedSegmentContext.getSegmentName(), is(currentSegment));
        assertThat("Segment was created from context with incorrect path", capturedSegmentContext.getSegmentPath(), is(path));
        assertEquals("Segment was created from context with incorrect size", 74, capturedSegmentContext.getCurrentSize());
        assertTrue("Segment was created from context without removed record in index",
                capturedSegmentContext.getIndex().searchForKey("key").isPresent());
        assertThat("Segment was created from context with incorrect offset in index for removed record",
                capturedSegmentContext.getIndex().searchForKey("key").get().getOffset(), is(33L));
        assertTrue("Segment was created from context without set record in index",
                capturedSegmentContext.getIndex().searchForKey("key1").isPresent());
        assertThat("Segment was created from context with incorrect offset in index for set record",
                capturedSegmentContext.getIndex().searchForKey("key1").get().getOffset(), is(44L));
        assertTrue("Segment was created from context without empty set record in index",
                capturedSegmentContext.getIndex().searchForKey("key2").isPresent());
        assertThat("Segment was created from context with incorrect offset in index for empty set record",
                capturedSegmentContext.getIndex().searchForKey("key2").get().getOffset(), is(62L));


        //assert that table context was updated
        assertThat("Current segment of table context wasn't updated", context.currentTableContext().getCurrentSegment().getName(), is(currentSegment));
        assertThat("Key from previous segment has incorrect segment name in table index",
                context.currentTableContext().getTableIndex().searchForKey("key0").map(Segment::getName).orElse(""), is("previous"));
        assertThat("Removed record has incorrect segment name in table index",
                context.currentTableContext().getTableIndex().searchForKey("key").map(Segment::getName).orElse(""), is(currentSegment));
        assertThat("Set record has incorrect segment name in table index",
                context.currentTableContext().getTableIndex().searchForKey("key1").map(Segment::getName).orElse(""), is(currentSegment));
        assertThat("Empty set record has incorrect segment name in table index",
                context.currentTableContext().getTableIndex().searchForKey("key2").map(Segment::getName).orElse(""), is(currentSegment));
    }

    static class TestRecord implements WritableDatabaseRecord {
        private final byte[] key;
        private final int keySize;
        private final byte[] value;
        private final int valueSize;
        private final int size;
        private final boolean isValuePresented;

        TestRecord(byte[] key, int keySize, byte[] value, int valueSize, int size, boolean isValuePresented) {
            this.key = key;
            this.keySize = keySize;
            this.value = value;
            this.valueSize = valueSize;
            this.size = size;
            this.isValuePresented = isValuePresented;
        }

        @Override
        public byte[] getKey() {
            return key;
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public boolean isValuePresented() {
            return isValuePresented;
        }

        @Override
        public int getKeySize() {
            return keySize;
        }

        @Override
        public int getValueSize() {
            return valueSize;
        }
    }

    static class SegmentStub implements Segment {
        private final String name;

        public SegmentStub(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean write(String objectKey, byte[] objectValue) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<byte[]> read(String objectKey) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isReadOnly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean delete(String objectKey) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

}