package com.yokalona.array.persitent.exceptions;

import com.yokalona.array.persitent.PersistentArray;
import com.yokalona.array.persitent.util.Version;

public class IncompatibleVersionException extends RuntimeException {
    public IncompatibleVersionException(Version version) {
        super("Provided version: " + version + " is not compatible with: " + PersistentArray.VERSION);
    }
}
