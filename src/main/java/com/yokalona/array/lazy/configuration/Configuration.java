package com.yokalona.array.lazy.configuration;

import com.yokalona.array.lazy.subscriber.Subscriber;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

// cannot write more than memory size

public record Configuration(File file, Chunked read, Chunked write, Chunked memory, List<Subscriber> subscribers) {

    public Configuration {
        if (read.size() > memory.size()) throw new ReadChunkLimitExceededException();
        if (write.size() > memory.size()) throw new WriteChunkLimitExceededException();
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
        ChunkLeft memory(Chunked memory);
    }

    public interface ChunkLeft {
        ChunkLeft addSubscriber(Subscriber subscriber);

        ReadLeft write(Chunked write);

        WriteLeft read(Chunked read);
    }

    public static final class ConfigurationBuilder implements MemoryLeft, ChunkLeft {
        private final File file;
        private Chunked memory;
        private final List<Subscriber> subscribers = new ArrayList<>();

        public ConfigurationBuilder(File file) {
            this.file = file;
        }

        public ConfigurationBuilder
        memory(Chunked memory) {
            this.memory = memory;
            return this;
        }

        @Override
        public ChunkLeft addSubscriber(Subscriber subscriber) {
            subscribers.add(subscriber);
            return this;
        }

        public WriteLeft
        read(Chunked read) {
            return write -> new Configuration(file, read, write, memory, unmodifiableList(subscribers));
        }

        public ReadLeft
        write(Chunked write) {
            return read -> new Configuration(file, read, write, memory, unmodifiableList(subscribers));
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
