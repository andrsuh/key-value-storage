package ru.andrey.kvstorage.server.initialization.context;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import ru.andrey.kvstorage.server.index.impl.TableIndex;
import ru.andrey.kvstorage.server.initialization.TableInitializationContext;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializationContextImpl;
import ru.andrey.kvstorage.server.logic.Segment;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class TableInitializationContextTest {
    private final Path dbPath;

    @Rule
    public final TemporaryFolder dbFolder = new TemporaryFolder();

    public TableInitializationContextTest() throws IOException {
        dbFolder.create();
        dbPath = dbFolder.getRoot().toPath();
    }

    @Test
    public void createContext_ReturnValidName() {
        TableInitializationContext context =
                new TableInitializationContextImpl("test", dbPath, new TableIndex());
        assertEquals("test", context.getTableName());
    }

    @Test
    public void createContext_ReturnValidPath() {
        TableInitializationContext context =
                new TableInitializationContextImpl("test", dbPath, new TableIndex());
        assertEquals(dbPath.resolve("test"), context.getTablePath());
    }

    @Test
    public void updateCurrentSegment_WhenOnce_ReturnEqualSegment() {
        TableInitializationContext context =
                new TableInitializationContextImpl("test", dbPath, new TableIndex());
        Segment segment = Mockito.mock(Segment.class);
        context.updateCurrentSegment(segment);
        assertEquals(segment, context.getCurrentSegment());
    }

    @Test
    public void updateCurrentSegment_WhenTwice_ReturnEqualSegments() {
        TableInitializationContext context =
                new TableInitializationContextImpl("table", dbPath, new TableIndex());
        Segment segment1 = Mockito.mock(Segment.class);
        Segment segment2 = Mockito.mock(Segment.class);
        context.updateCurrentSegment(segment1);
        assertEquals(segment1, context.getCurrentSegment());
        context.updateCurrentSegment(segment2);
        assertEquals(segment2, context.getCurrentSegment());
    }
}
