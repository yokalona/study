package com.yokalona.array.lazy;

import java.io.IOException;
import java.io.RandomAccessFile;

public interface DataLayout {
    void seek(int index, RandomAccessFile raf) throws IOException;
}
