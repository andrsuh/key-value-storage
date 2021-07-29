package ru.andrey.kvstorage.server.initialization.impl;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.TableIndex;
import ru.andrey.kvstorage.server.logic.Table;
import ru.andrey.kvstorage.server.logic.impl.TableImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class TableInitializerTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private static final MockedStatic<TableImpl> tableImplStaticMock = mockStatic(TableImpl.class);
    private final SegmentInitializer segmentInitializer = mock(SegmentInitializer.class);
    private final TableInitializer tableInitializer = new TableInitializer(segmentInitializer);

    @AfterClass
    public static void down() {
        tableImplStaticMock.close();
    }

    @Test
    public void invalidTableInContext_exceptionIsThrown(){
        try {
            tableInitializer.perform(new InitializationContextImpl(null, null,
                    new TableInitializationContextImpl("table", temporaryFolder.getRoot().toPath(), new TableIndex()),
                    null));
            fail();
        } catch (DatabaseException e) {
            verifyNoMoreInteractions(segmentInitializer);
        }
    }

    @Test
    public void validTableInContextNoSegments_noInitializationStarted_tableInContext() throws DatabaseException, IOException {
        temporaryFolder.newFolder("test_table");

        ArgumentCaptor<TableInitializationContextImpl> tableContextCaptor = ArgumentCaptor.forClass(TableInitializationContextImpl.class);
        tableImplStaticMock.when(() -> TableImpl.initializeFromContext(tableContextCaptor.capture())).thenReturn(new TableStub("test_table"));

        //act
        TableInitializationContextImpl tableContext = new TableInitializationContextImpl("test_table", temporaryFolder.getRoot().toPath(), new TableIndex());
        DatabaseInitializationContextImpl dbContext = new DatabaseInitializationContextImpl("db_files", temporaryFolder.getRoot().toPath());
        dbContext.addTable(new TableStub("table2"));
        InitializationContextImpl context = new InitializationContextImpl(null,
                dbContext,
                tableContext,
                null);
        tableInitializer.perform(context);

        assertTrue("Empty table wasn't added to db context", context.currentDbContext().getTables().containsKey("test_table"));
        assertTrue("Previous table was removed from db context", context.currentDbContext().getTables().containsKey("table2"));
        assertThat("Context has more/less than 2 tables", context.currentDbContext().getTables().size(), is(2));

        verifyNoMoreInteractions(segmentInitializer);

        List<TableInitializationContextImpl> tableContextToCreate = tableContextCaptor.getAllValues();
        assertThat(tableContextToCreate.size(), is(1));
        assertThat("Creation from context was initialized for table with incorrect name", tableContextToCreate.get(0).getTableName(), is("test_table"));
    }


    @Test
    public void validTableInContext_tableIsCreatedAccordingToTimestamps_dbIndexUpdated() throws DatabaseException, IOException {
        File testTable = temporaryFolder.newFolder("testTable");
        Files.createFile(testTable.toPath().resolve("testTable_0000071"));
        Files.createFile(testTable.toPath().resolve("testTable_0000001"));
        Files.createFile(testTable.toPath().resolve("testTable_0000002"));

        ArgumentCaptor<TableInitializationContextImpl> tableContextCaptor = ArgumentCaptor.forClass(TableInitializationContextImpl.class);
        tableImplStaticMock.when(() -> TableImpl.initializeFromContext(tableContextCaptor.capture())).thenReturn(new TableStub("testTable"));

        ArgumentCaptor<InitializationContextImpl> downstreamContextCaptor = ArgumentCaptor.forClass(InitializationContextImpl.class);
        doNothing().when(segmentInitializer).perform(downstreamContextCaptor.capture());

        //act
        TableInitializationContextImpl tableContext = new TableInitializationContextImpl("testTable", temporaryFolder.getRoot().toPath(), new TableIndex());
        DatabaseInitializationContextImpl dbContext = new DatabaseInitializationContextImpl("db_files", temporaryFolder.getRoot().toPath());
        dbContext.addTable(new TableStub("table2"));
        InitializationContextImpl context = new InitializationContextImpl(null,
                dbContext,
                tableContext,
                null);
        tableInitializer.perform(context);

        assertTrue("Not empty table wasn't added to db context", context.currentDbContext().getTables().containsKey("testTable"));
        assertTrue("Previous table was removed from db context", context.currentDbContext().getTables().containsKey("table2"));
        assertThat("Db context has not 2 tables", context.currentDbContext().getTables().size(), is(2));

        List<InitializationContextImpl> capturedSegmentContexts = downstreamContextCaptor.getAllValues();
        assertThat("Segment initialization wasn't started for 3 segments", capturedSegmentContexts.size(), is(3));
        assertThat("Segment with the earliest timestamp in name wasn't initialized first", capturedSegmentContexts.get(0).currentSegmentContext().getSegmentName(), is("testTable_0000001"));
        assertThat("Segment initialization is started not according timestamps in names", capturedSegmentContexts.get(1).currentSegmentContext().getSegmentName(), is("testTable_0000002"));
        assertThat("Segment initialization is started not according timestamps in names", capturedSegmentContexts.get(2).currentSegmentContext().getSegmentName(), is("testTable_0000071"));

        List<TableInitializationContextImpl> tableContextToCreate = tableContextCaptor.getAllValues();
        assertThat("Created from context table has invalid name", tableContextToCreate.get(0).getTableName(), is("testTable"));
    }

    static class TableStub implements Table {
        private final String tableName;

        TableStub(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public String getName() {
            return tableName;
        }

        @Override
        public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        }

        @Override
        public Optional<byte[]> read(String objectKey) throws DatabaseException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(String objectKey) {
        }
    }

}