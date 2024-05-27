package com.yokalona.array.persitent.subscriber;

import java.util.Arrays;

public class CountingSubscriber implements Subscriber {

    private final long[] counters = new long[Counter.values().length * 2];

    @Override
    public void
    onSerialized(int index) {
        counters[Counter.SERIALIZATIONS.ordinal()] ++;
    }

    @Override
    public void
    onDeserialized(int index) {
        counters[Counter.DESERIALIZATIONS.ordinal()] ++;
    }

    @Override
    public void
    onCacheMiss(int current) {
        counters[Counter.CACHE_MISS.ordinal()] ++;
    }

    @Override
    public void
    onWriteCollision(int current, int next) {
        counters[Counter.WRITE_COLLISIONS.ordinal()] ++;
    }

    @Override
    public void onChunkSerialized() {
        counters[Counter.CHUNK_SERIALIZED.ordinal()] ++;
    }

    @Override
    public void onChunkDeserialized() {
        counters[Counter.CHUNK_DESERIALIZED.ordinal()] ++;
    }

    @Override
    public void onFileCreated() {
        counters[Counter.FILE_CREATED.ordinal()] = System.currentTimeMillis();
    }

    public void
    reset() {
        long fileCreationTime = counters[Counter.FILE_CREATED.ordinal()];
        for (int counter = 0; counter < Counter.values().length; counter ++) counters[counter + Counter.values().length] += counters[counter];
        Arrays.fill(counters, 0, Counter.values().length, 0);
        counters[Counter.FILE_CREATED.ordinal()] = fileCreationTime;
    }

    public long
    get(Counter counter) {
        return counters[counter.ordinal()];
    }

    public float
    average(Counter counter, int on) {
        return ((float) counters[Counter.valueOf(counter.name()).ordinal() + Counter.values().length]) / on;
    }

    public enum Counter {
        SERIALIZATIONS, CHUNK_SERIALIZED, DESERIALIZATIONS, CHUNK_DESERIALIZED, CACHE_MISS, WRITE_COLLISIONS, FILE_CREATED
    }

}
