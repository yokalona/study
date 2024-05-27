package com.yokalona.array.persitent.io;

import com.yokalona.array.persitent.PersistentArray;
import com.yokalona.array.persitent.io.DataLayout;
import com.yokalona.array.persitent.serializers.TypeDescriptor;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Describes a fixed object data layout. Each object in such a layout has a fixed size it can occupy in the output
 * file. This layout can be beneficial for fixed size data types, such as integers or composite data types
 * consistent with fixed size data types. For large data sets, this data layout saves space on disk, however, for
 * small arrays it will create unnecessary overhead.
 *
 * @param descriptor
 */
public record FixedObjectLayout(TypeDescriptor<?> descriptor) implements DataLayout {
    @Override
    public void seek(int index, RandomAccessFile raf) throws IOException {
        raf.seek(((long) index) * descriptor.size() + PersistentArray.HEADER_SIZE);
    }

    @Override
    public byte
    mode() {
        return 1;
    }
}
