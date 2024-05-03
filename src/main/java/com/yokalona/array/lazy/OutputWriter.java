package com.yokalona.array.lazy;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

class OutputWriter implements AutoCloseable {

    private final OutputStream output;
    private final byte[] buffer;
    private final boolean keepAlive;
    private int position;

    public OutputWriter(OutputStream output, int size, boolean keepAlive) {
        this.output = output;
        this.buffer = new byte[size];
        this.keepAlive = keepAlive;
    }

    public OutputWriter(RandomAccessFile raf, int size) throws IOException {
        this(new FileOutputStream(raf.getFD()), size, true);
    }

    public void
    write(byte[] data) throws IOException {
        int written = 0;
        while (position + data.length - written > buffer.length) {
            written += write(data, written, buffer.length - position);
            flush();
        }
        write(data, written, data.length - written);
    }

    public int
    write(byte[] data, int offset, int length) {
        System.arraycopy(data, offset, buffer, position, length);
        position += length;
        return length;
    }

    public void
    flush() throws IOException {
        output.write(buffer, 0, position);
        position = 0;
    }

    @Override
    public void
    close() throws Exception {
        flush();
        if (!keepAlive) output.close();
    }
}
