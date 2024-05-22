package com.yokalona.tree.b;

import com.yokalona.array.lazy.PersistentArray;
import com.yokalona.array.lazy.serializers.Serializer;
import com.yokalona.array.lazy.serializers.SerializerStorage;
import com.yokalona.array.lazy.serializers.TypeDescriptor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static com.yokalona.array.lazy.configuration.Chunked.chunked;
import static com.yokalona.array.lazy.configuration.Chunked.linear;
import static com.yokalona.array.lazy.configuration.File.file;
import static com.yokalona.array.lazy.configuration.Configuration.InMemory.memorise;
import static com.yokalona.array.lazy.configuration.Configuration.configure;

@State(Scope.Benchmark)
public class ArrayWriteTest {

    static final int OPERATIONS = 10000;

    static TypeDescriptor<CompactInteger> compact = new TypeDescriptor<>(5, CompactInteger.class);

    @State(Scope.Benchmark)
    public static class ExecutionPlan {

        @Param({"32", "1024", "1048576", "33554432"})
        public int size;

        @Param({"32", "1024", "16384", "1048576"})
        public int memory;

        public int [] data;
        public CompactInteger [] integers;
        public PersistentArray<CompactInteger> array;

        public int next;

        @Setup(Level.Trial)
        public void
        setUp() throws IOException {
            byte[] bytes = new byte[5];
            SerializerStorage.register(compact, new Serializer<>() {

                @Override
                public byte[] serialize(CompactInteger value) {
                    if (value == null) {
                        bytes[0] = 0xF;
                        return bytes;
                    } else bytes[0] = 0x0;
                    int length = bytes.length;
                    int vals = value.get();
                    for (int i = 0; i < bytes.length - 1; i++) {
                        bytes[length - i - 1] = (byte) (vals & 0xFF);
                        vals >>= 8;
                    }
                    return bytes;
                }

                @Override
                public CompactInteger deserialize(byte[] bytes) {
                    return deserialize(bytes, 0);
                }

                @Override
                public CompactInteger deserialize(byte[] bytes, int offset) {
                    if (bytes[offset] == 0xF) return null;
                    int value = 0;
                    for (int index = offset + 1; index < offset + 5; index++) {
                        value = (value << 8) + (bytes[index] & 0xFF);
                    }
                    return new CompactInteger(value);
                }

                @Override
                public TypeDescriptor<CompactInteger> descriptor() {
                    return compact;
                }
            });

            Path path = Files.createTempDirectory("jmhwrite");
            this.array = new PersistentArray<>(size, compact, new PersistentArray.FixedObjectLayout(compact),
                    configure(
                            file(path.resolve("array.la"))
                            .cached())
                            .memory(memorise(memory))
                            .read(linear())
                            .write(linear()));
            data = new int[size];
            integers = new CompactInteger[size];
            for (int index = 0; index < size; index ++) data[index] = index;
            shuffle(data);
            for (int index = 0; index < size; index ++) integers[index] = new CompactInteger(index);
        }

        @TearDown(Level.Trial)
        public void
        close() throws IOException {
            array.close();
            System.err.println("File size: " + getSize(Files.size(array.configuration.file().path())));
        }

    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OperationsPerInvocation(OPERATIONS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 500, batchSize = OPERATIONS)
    @Measurement(iterations = 500, batchSize = OPERATIONS)
    public void array_linear(ExecutionPlan executionPlan) {
        int next = executionPlan.next = (executionPlan.next + 1) % executionPlan.size;
        executionPlan.array.set(executionPlan.data[next], executionPlan.integers[next]);
    }

    public static class CompactInteger {
        private final int value;

        CompactInteger(int value) {
            this.value = value;
        }

        public int
        get() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CompactInteger that = (CompactInteger) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }

    public static void
    shuffle(int[] array) {
        for (int idx = array.length - 1; idx > 0; idx--) {
            int element = (int) Math.floor(Math.random() * (idx + 1));
            swap(array, idx, element);
        }
    }

    private static void
    swap(int[] arr, int left, int right) {
        int tmp = arr[left];
        arr[left] = arr[right];
        arr[right] = tmp;
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .forks(1)
                .resultFormat(ResultFormatType.JSON)
                .result("benchmarks/output-array.json")
                .include(ArrayWriteTest.class.getSimpleName())
                .build();

        new Runner(options).run();
    }

    static long kilo = 1024;
    static long mega = kilo * kilo;
    static long giga = mega * kilo;
    static long tera = giga * kilo;

    public static String getSize(long size) {
        double kb = (double) size / kilo, mb = kb / kilo, gb = mb / kilo, tb = gb / kilo;
        if (size < kilo) return size + " b";
        else if (size < mega) return String.format("%.2f Kb", kb);
        else if (size < giga) return String.format("%.2f Mb", mb);
        else if (size < tera) return String.format("%.2f Gb", gb);
        else return String.format("%.2f Tb", tb);
    }

}