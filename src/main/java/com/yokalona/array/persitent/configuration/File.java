package com.yokalona.array.persitent.configuration;

import java.nio.file.Path;

import static com.yokalona.array.persitent.configuration.File.Mode.RW;

public record File(Path path, Mode mode, int buffer, boolean cached) {
    public enum Mode {
        R("read only"), RW("read/write"), RWS("sync read/write"), RWD("dsync read/write");

        public final String explanation;

        Mode(String explanation) {
            this.explanation = explanation;
        }

        public String
        mode() {
            return this.toString().toLowerCase();
        }
    }

    public static FileConfigurer
    file(Path path) {
        return new FileConfigurer(path);
    }

    public static class FileConfigurer {
        private final Path path;
        private Mode mode;
        private int buffer;

        public FileConfigurer(Path path) {
            this.mode = RW;
            this.path = path;
            this.buffer = 8192;
        }

        public FileConfigurer
        mode(Mode mode) {
            this.mode = mode;
            return this;
        }

        public FileConfigurer
        buffer(int buffer) {
            this.buffer = buffer;
            return this;
        }

        public File
        cached() {
            return new File(this.path, this.mode, this.buffer, true);
        }

        public File
        uncached() {
            return new File(this.path, this.mode, this.buffer, false);
        }

    }
}
