package com.yokalona.tree.b.array;

import com.yokalona.array.persitent.io.FixedObjectLayout;
import com.yokalona.array.persitent.PersistentArray;
import com.yokalona.array.persitent.debug.CompactInteger;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static com.yokalona.array.persitent.configuration.Chunked.chunked;
import static com.yokalona.array.persitent.configuration.ChunkedRead.read;
import static com.yokalona.array.persitent.configuration.ChunkedWrite.write;
import static com.yokalona.array.persitent.configuration.Configuration.configure;
import static com.yokalona.array.persitent.configuration.File.file;
import static com.yokalona.tree.b.Helper.shuffle;

@State(Scope.Benchmark)
public class GetExecutionPlan {

    @Param({"32", "1024", "1048576"})
    public int size;
    @Param({".01", ".05", ".1", ".25"})
    public float factor;

    public int next;
    public Path path;
    public int[] random;
    public int[] linear;
    public int[] collisional;

    public PersistentArray<CompactInteger> array;

    @Setup(Level.Trial)
    public void
    setUp() throws IOException {
        path = Files.createTempDirectory("jmh");
        this.array = new PersistentArray<>(size, CompactInteger.descriptor, FixedObjectLayout::new,
                configure(file(path.resolve("array.linear")).cached())
                        .memory(chunked(factor()))
                        .read(read().breakOnLoaded().chunked(factor()))
                        .write(write().chunked(factor())));
        this.linear = new int[size];
        for (int index = 0; index < size; index++) {
            linear[index] = index;
            array.set(index, new CompactInteger(index));
        }
    }

    @Setup(Level.Iteration)
    public void
    produceData() {
        this.random = Arrays.copyOf(linear, linear.length);
        shuffle(random);
        this.collisional = Arrays.copyOf(random, factor());
        shuffle(collisional);
    }

    @TearDown(Level.Trial)
    public void
    close() throws IOException {
        array.close();
        try (var folder = Files.list(path)) {
            folder.map(Path::toFile).forEach(file -> {
                var ignore = file.delete();
            });
        }
    }

    public int
    factor() {
        return (int) Math.max(1, size * factor);
    }

    public int
    nextRandom() {
        return random[(next++) % random.length];
    }

    public int
    nextLinear() {
        return linear[(next++) % linear.length];
    }

    public int
    nextCollisional() {
        return collisional[(next++) % collisional.length];
    }

}
