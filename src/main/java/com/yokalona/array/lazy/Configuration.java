package com.yokalona.array.lazy;

import java.nio.file.Path;

public record Configuration(File file, Chunked read, Chunked write) {

    public static ConfigurationBuilder
    configure(File file) {
        return new ConfigurationBuilder(file);
    }

    public static final class ConfigurationBuilder {
        private final File file;

        public interface WriteLeft {
            Configuration write(Chunked write);
        }

        public interface ReadLeft {
            Configuration read(Chunked read);
        }

        public ConfigurationBuilder(File file) {
            this.file = file;
        }

        public WriteLeft
        read(Chunked read) {
            return write -> new Configuration(file, read, write);
        }

        public ReadLeft
        write(Chunked write) {
            return read -> new Configuration(file, read, write);
        }

    }

    public record File(Path path, Mode mode, int buffer, boolean cached) {
        enum Mode {
            R, RW, RWS, RWD;

            String
            mode() {
                return this.toString().toLowerCase();
            }
        }
    }

    public record Chunked(boolean chunked, int size) {
        public static Chunked
        linear() {
            return new Chunked(false, 0);
        }

        public static Chunked
        chunked(int size) {
            assert size > 0;
            return new Chunked(true, size);
        }
    }
}
