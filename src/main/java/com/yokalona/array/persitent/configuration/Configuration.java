package com.yokalona.array.persitent.configuration;

import com.yokalona.array.persitent.exceptions.ReadChunkLimitExceededException;
import com.yokalona.array.persitent.exceptions.WriteChunkLimitExceededException;
import com.yokalona.array.persitent.subscriber.Subscriber;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public record Configuration(File file, ChunkedRead read, ChunkedWrite write, Chunked memory, List<Subscriber> subscribers) {

    public Configuration {
        if (read.size() > memory.size()) throw new ReadChunkLimitExceededException();
        if (write.size() > memory.size()) throw new WriteChunkLimitExceededException();
    }

    public static MemoryLeft
    configure(File file) {
        return new ConfigurationBuilder(file);
    }

    public interface WriteLeft {
        Configuration write(ChunkedWrite write);
    }

    public interface ReadLeft {
        Configuration read(ChunkedRead read);
    }

    public interface MemoryLeft {
        ChunkLeft memory(Chunked memory);
    }

    public interface ChunkLeft {
        ChunkLeft addSubscriber(Subscriber subscriber);

        ReadLeft write(ChunkedWrite write);

        WriteLeft read(ChunkedRead read);
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
        read(ChunkedRead read) {
            return write -> new Configuration(file, read, write, memory, unmodifiableList(subscribers));
        }

        public ReadLeft
        write(ChunkedWrite write) {
            return read -> new Configuration(file, read, write, memory, unmodifiableList(subscribers));
        }

    }

}
