package com.yokalona.array.persitent.io;

import java.io.IOException;
import java.io.RandomAccessFile;

public interface DataLayout {
    void seek(int index, RandomAccessFile raf) throws IOException;
    byte mode();
}
