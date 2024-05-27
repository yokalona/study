package com.yokalona.array.persitent.io;

import java.io.IOException;
import java.io.RandomAccessFile;

public class InputReader {

    private final byte[] buffer;
    private final RandomAccessFile raf;
    private int pointer = 0;

    public InputReader(byte[] buffer, RandomAccessFile raf) {
        this.raf = raf;
        this.buffer = buffer;
        pointer = buffer.length;
    }

    public void
    read(byte[] data) throws IOException {
        assert pointer >= 0 && pointer <= buffer.length;

        int available = buffer.length - pointer;
        if (available >= data.length) {
            System.arraycopy(buffer, pointer, data, 0, data.length);
            pointer += data.length;
        } else {
            System.arraycopy(buffer, pointer, data, 0, available);
            refill();
            for (int i = 0; i < (data.length - available) / buffer.length; i++) {
                System.arraycopy(buffer, 0, data, available, buffer.length);
                available += buffer.length;
                refill();
            }
            System.arraycopy(buffer, 0, data, available, data.length - available);
            pointer += (data.length - available);
        }
    }

    public void
    refill() throws IOException {
        raf.read(buffer);
        pointer = 0;
    }

    public void
    invalidate() {
        pointer = buffer.length;
    }

}
