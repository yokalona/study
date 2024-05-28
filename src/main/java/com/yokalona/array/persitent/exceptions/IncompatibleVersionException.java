package com.yokalona.array.persitent.exceptions;

import com.yokalona.array.persitent.PersistentArray;
import com.yokalona.array.persitent.serializers.Version;

public class IncompatibleVersionException extends RuntimeException {
    public IncompatibleVersionException(Version version) {
        super("Provided version: %s is not compatible with: %s".formatted(version, PersistentArray.VERSION));
    }
}
