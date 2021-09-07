package ru.andrey.kvstorage.server.initialization.context;

import org.junit.Test;
import ru.andrey.kvstorage.server.initialization.SegmentInitializationContext;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializationContextImpl;

import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class SegmentInitializationContextTest {

    private final Path tablePath = Path.of("testtablepath");

    @Test
    public void createContext_ReturnValidName() {
        SegmentInitializationContext context =
                new SegmentInitializationContextImpl("segment", tablePath, 100);
        assertEquals("segment", context.getSegmentName());
    }

    @Test
    public void createContext_ReturnValidPath() {
        SegmentInitializationContext context =
                new SegmentInitializationContextImpl("segment", tablePath, 100);
        assertEquals(tablePath.resolve("segment"), context.getSegmentPath());
    }

    @Test
    public void createContext_ReturnValidSize() {
        SegmentInitializationContext context =
                new SegmentInitializationContextImpl("segment", tablePath, 100);
        assertEquals(100, context.getCurrentSize());
    }
}
