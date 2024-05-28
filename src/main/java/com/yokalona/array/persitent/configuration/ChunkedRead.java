package com.yokalona.array.persitent.configuration;

public record ChunkedRead(int size, boolean breakOnLoaded, boolean forceReload) {

    public ChunkedRead {
        assert size > 0;
    }

    public static ChunkedReadBuilder
    read() {
        return new ChunkedReadBuilder();
    }

    public static class ChunkedReadBuilder {
        private boolean breakOnLoaded;
        private boolean forceReload;

        public ChunkedRead
        chunked(int size) {
            assert size > 0;
            return new ChunkedRead(size, breakOnLoaded, forceReload);
        }

        public ChunkedRead
        linear() {
            return new ChunkedRead(1, breakOnLoaded, forceReload);
        }

        public ChunkedReadBuilder
        breakOnLoaded() {
            this.breakOnLoaded = true;
            return this;
        }

        public ChunkedReadBuilder
        forceReload() {
            this.forceReload = true;
            return this;
        }
    }

}
