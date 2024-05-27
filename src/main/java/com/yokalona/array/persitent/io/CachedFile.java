package com.yokalona.array.persitent.io;

import com.yokalona.TestOnly;
import com.yokalona.array.persitent.configuration.File;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

public class CachedFile implements AutoCloseable {
    private final File configuration;
    private RandomAccessFile file;

    public CachedFile(File configuration) {
        this.configuration = configuration;
    }

    @TestOnly
    public CachedFile(File configuration, RandomAccessFile file) {
        this.file = file;
        this.configuration = configuration;
    }

    public RandomAccessFile
    get() {
        try {
            return (!configuration.cached() || file == null)
                    ? file = new RandomAccessFile(configuration.path().toFile(), configuration.mode().mode())
                    : file;
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
