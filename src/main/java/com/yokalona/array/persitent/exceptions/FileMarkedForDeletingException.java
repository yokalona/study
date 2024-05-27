package com.yokalona.array.persitent.exceptions;

public class FileMarkedForDeletingException extends RuntimeException {
    public FileMarkedForDeletingException() {
        super("File is marked for deletion");
    }
}
