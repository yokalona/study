package com.yokalona.array.persitent.io;

import com.yokalona.array.persitent.serializers.TypeDescriptor;

import java.io.InputStream;

public interface LayoutProvider {
    DataLayout provide(TypeDescriptor<?> descriptor);

    static LayoutProvider
    which(byte format, InputStream input) {
        return switch (format & 0b0000011) {
            case 1 -> FixedObjectLayout::new;
            default -> throw new UnsupportedOperationException();
        };
    }
}
