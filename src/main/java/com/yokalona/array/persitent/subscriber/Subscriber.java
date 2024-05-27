package com.yokalona.array.persitent.subscriber;

public interface Subscriber {

    default void
    onSerialized(int index) {
    }

    default void
    onChunkSerialized() {
    }

    default void
    onDeserialized(int index) {
    }

    default void
    onChunkDeserialized() {
    }

    default void
    onCacheMiss(int current) {
    }

    default void
    onWriteCollision(int prior, int current) {
    }

    default void
    onFileCreated() {
    }

    default void
    onChunkResized(ChunkType type, int prior, int current) {
    }

}

