package com.yokalona.array.lazy;

import com.yokalona.array.lazy.serializers.Serializer;
import com.yokalona.array.lazy.serializers.SerializerStorage;
import com.yokalona.array.lazy.serializers.TypeDescriptor;

public record CompactInteger(int value) {

    public static TypeDescriptor<CompactInteger> descriptor = new TypeDescriptor<>(5, CompactInteger.class);
    static {
        SerializerStorage.register(descriptor, new CompactIntegerSerializer(new byte[5]));
    }

    public static CompactInteger
    compact(int value) {
        return new CompactInteger(value);
    }

    private record CompactIntegerSerializer(byte[] bytes) implements Serializer<CompactInteger> {

        @Override
        public byte[] serialize(CompactInteger value) {
            if (value == null) {
                bytes[0] = 0xF;
                return bytes;
            } else bytes[0] = 0x0;
            int length = bytes.length;
            int vals = value.value;
            for (int i = 0; i < bytes.length - 1; i++) {
                bytes[length - i - 1] = (byte) (vals & 0xFF);
                vals >>= 8;
            }
            return bytes;
        }

        @Override
        public CompactInteger deserialize(byte[] bytes) {
            return deserialize(bytes, 0);
        }

        @Override
        public CompactInteger deserialize(byte[] bytes, int offset) {
            if (bytes[offset] == 0xF) return null;
            int value = 0;
            for (int index = offset + 1; index < offset + 5; index++) {
                value = (value << 8) + (bytes[index] & 0xFF);
            }
            return new CompactInteger(value);
        }

        @Override
        public TypeDescriptor<CompactInteger> descriptor() {
            return descriptor;
        }
    }
}
