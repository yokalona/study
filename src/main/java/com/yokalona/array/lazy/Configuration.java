package com.yokalona.array.lazy;

import java.nio.file.Path;

import static com.yokalona.array.lazy.Configuration.File.Mode.RW;

public record Configuration(File file, Chunked read, Chunked write, InMemory memory) {

    public Configuration {
        memory.count = Math.max(memory.count, Math.max(read.size, write.size));
    }

    public static MemoryLeft
    configure(File file) {
        return new ConfigurationBuilder(file);
    }

    public interface WriteLeft {
        Configuration write(Chunked write);
    }

    public interface ReadLeft {
        Configuration read(Chunked read);
    }

    public interface MemoryLeft {
        ChunkLeft memory(InMemory memory);
    }

    public interface ChunkLeft {
        ReadLeft write(Chunked write);

        WriteLeft read(Chunked read);
    }

    public static final class ConfigurationBuilder implements MemoryLeft, ChunkLeft {
        private final File file;
        private InMemory memory = new InMemory(-1);

        public ConfigurationBuilder(File file) {
            this.file = file;
        }

        public ConfigurationBuilder
        memory(InMemory memory) {
            this.memory = memory;
            return this;
        }

        public WriteLeft
        read(Chunked read) {
            return write -> new Configuration(file, read, write, memory);
        }

        public ReadLeft
        write(Chunked write) {
            return read -> new Configuration(file, read, write, memory);
        }

    }

    public record File(Path path, Mode mode, int buffer, boolean cached) {
        public enum Mode {
            R, RW, RWS, RWD;

            String
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

    public record Chunked(boolean chunked, int size, boolean hot) {
        public static Chunked
        linear() {
            return new Chunked(false, 0, false);
        }

        public static Chunked
        chunked(int size) {
            if (size > 0) return new Chunked(true, size, false);
            else return linear();
        }

        public static Chunked
        chunked(int size, boolean hot) {
            assert size > 0;
            return new Chunked(true, size, true);
        }
    }

    public static final class InMemory {
        private int count;

        private InMemory(int count) {
            this.count = count;
        }

        public static InMemory
        memorise(int count) {
            assert count > 0;

            return new InMemory(count);
        }

        public static InMemory
        none() {
            return new InMemory(1);
        }

        public int
        count() {
            return count;
        }

    }

}
