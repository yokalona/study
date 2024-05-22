package com.yokalona.array.lazy.subscriber;

public interface Subscriber {

    default void
    onSerialization(int index) {
    }

    default void
    onDeserialization(int index) {
    }

    default void
    onLoad(int index) {
    }

    default void
    onUnload(int index) {
    }
}
