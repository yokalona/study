package com.yokalona.array.lazy;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

class CachedFile implements AutoCloseable {
    private RandomAccessFile file;
    private final Configuration.File configuration;

    public CachedFile(Configuration.File configuration) {
        this.configuration = configuration;
    }

    public RandomAccessFile
    get() {
        try {
            if (!configuration.cached() || file == null)
                return file = new RandomAccessFile(configuration.path().toFile(), configuration.mode().mode());
            else return file;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void
    close() {
        try {
            if (!configuration.cached()) file.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void
    closeFile() {
        try {
            if (file != null) file.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public RandomAccessFile
    peek() {
        return file;
    }
}
