package com.yokalona.array.persitent.subscriber;

import java.util.Arrays;

public class CountingSubscriber implements Subscriber {
    private final long[] counters = new long[Counter.values().length * 2];

    @Override
    public void
    onSerialized(int index) {
        inc(Counter.SERIALIZATIONS);
    }

    @Override
    public void
    onDeserialized(int index) {
        inc(Counter.DESERIALIZATIONS);
    }

    @Override
    public void
    onCacheMiss(int current) {
        inc(Counter.CACHE_MISS);
    }

    @Override
    public void
    onWriteCollision(int current, int next) {
        inc(Counter.WRITE_COLLISIONS);
    }

    @Override
    public void
    onChunkSerialized() {
        inc(Counter.CHUNK_SERIALIZATIONS);
    }

    @Override
    public void
    onChunkDeserialized() {
        inc(Counter.CHUNK_DESERIALIZATIONS);
    }

    @Override
    public void
    onFileCreated() {
        counters[Counter.FILE_CREATED.ordinal()] = System.currentTimeMillis();
    }

    public void
    reset() {
        Arrays.fill(counters, 0, Counter.values().length, 0);
    }

    public long
    get(Counter counter) {
        return counters[counter.ordinal()];
    }

    public float
    average(Counter counter, int on) {
        return ((float) counters[Counter.valueOf(counter.name()).ordinal() + Counter.values().length]) / on;
    }

    private void
    inc(Counter counter) {
        counters[counter.ordinal()]++;
        counters[counter.ordinal() + Counter.values().length] ++;
    }

    public String
    toString() {
        StringBuilder report = new StringBuilder("Iterations:\n");
        for (Counter counter : Counter.values()) {
            if (counter == Counter.FILE_CREATED) continue;
            report.append(String.format("\t%s: %d%n", counter.name(), counters[counter.ordinal()]));
        }
        report.append("Total:\n");
        for (Counter counter : Counter.values()) {
            if (counter == Counter.FILE_CREATED) continue;
            report.append(String.format("\t%s: %d%n", counter.name(), counters[counter.ordinal() + Counter.values().length]));
        }
        return report.toString();
    }

    public enum Counter {
        SERIALIZATIONS, CHUNK_SERIALIZATIONS, DESERIALIZATIONS, CHUNK_DESERIALIZATIONS, CACHE_MISS, WRITE_COLLISIONS, FILE_CREATED;
    }

}
