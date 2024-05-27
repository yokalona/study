package com.yokalona.array.persitent.exceptions;

public class DeserializationException extends RuntimeException {

    public DeserializationException(String message, Exception cause) {
        super(message, cause);
    }

}
