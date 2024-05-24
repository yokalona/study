package com.yokalona.array.lazy.configuration;

public class ReadChunkLimitExceededException extends RuntimeException {
    public ReadChunkLimitExceededException() {
        super("Read chunk cannot be bigger than configured memory limit");
    }
}
