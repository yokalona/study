package com.yokalona.array.lazy;

import com.yokalona.array.persitent.io.CachedFile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.yokalona.array.persitent.configuration.File.file;
import static org.junit.jupiter.api.Assertions.*;

class CachedFileTest {

    @Test
    public void
    testWriteIsInTheSameIfCached() throws IOException {
        Path path = Files.createTempDirectory("cachedfile");
        RandomAccessFile rw = new RandomAccessFile(path.resolve("write.tst").toFile(), "rw");
        try(CachedFile cachedFile = new CachedFile(file(path.resolve("write.tst")).cached(), rw)) {
            assertEquals(rw, cachedFile.get());
            assertEquals(rw, cachedFile.get());
            assertEquals(rw, cachedFile.peek());
        }
        assertTrue(rw.getChannel().isOpen());
        rw.close();
    }

    @Test
    public void
    testWriteIsNotInTheSameIfCached() throws IOException {
        Path path = Files.createTempDirectory("cachedfile");
        RandomAccessFile rw = new RandomAccessFile(path.resolve("write.tst").toFile(), "rw");
        try(CachedFile cachedFile = new CachedFile(file(path.resolve("write.tst")).uncached(), rw)) {
            assertNotEquals(rw, cachedFile.get());
            cachedFile.peek().close();
            assertNotEquals(rw, cachedFile.get());
            cachedFile.peek().close();
            assertNotEquals(rw, cachedFile.peek());
            cachedFile.peek().close();
        }
        assertTrue(rw.getChannel().isOpen());
        rw.close();
    }

}