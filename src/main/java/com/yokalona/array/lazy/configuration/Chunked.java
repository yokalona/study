package com.yokalona.array.lazy.configuration;

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
