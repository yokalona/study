package com.yokalona.array.persitent.configuration;

public record Chunked(boolean chunked, int size) {
    public static Chunked
    linear() {
        return new Chunked(false, 0);
    }

    public static Chunked
    chunked(int size) {
        if (size > 0) return new Chunked(true, size);
        else return linear();
    }

}

