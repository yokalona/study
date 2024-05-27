package com.yokalona.array.persitent.configuration;

public record ChunkedWrite(int size, boolean forceFlush) {

    public ChunkedWrite {
        assert size > 0;
    }

    public boolean
    chunked() {
        return size > 1;
    }

    public static ChunkedWriteBuilder
    write() {
        return new ChunkedWriteBuilder();
    }

    public static class ChunkedWriteBuilder {

        private boolean forceFlush;

        public ChunkedWriteBuilder
        forceFlush() {
            this.forceFlush = true;
            return this;
        }

        public ChunkedWrite
        chunked(int size) {
            return new ChunkedWrite(size, forceFlush);
        }

        public ChunkedWrite
        linear() {
            return new ChunkedWrite(1, forceFlush);
        }
    }
}
