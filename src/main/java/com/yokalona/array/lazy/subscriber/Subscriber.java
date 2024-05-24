package com.yokalona.array.lazy.subscriber;

public interface Subscriber {

    default void
    onSerialized(int index) {
    }

    default void
    onDeserialized(int index) {
    }
}
