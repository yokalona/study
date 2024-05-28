package com.yokalona.array.persitent.serializers;

public class LongSerializer implements Serializer<Long> {
    public static final Serializer<Long> INSTANCE = new LongSerializer();

    private LongSerializer() {}

    @Override
    public byte[] serialize(Long value) {
        byte[] bytes = new byte[descriptor().size()];
        if (value == null) {
            bytes[0] = 0xF;
            return bytes;
        }
        int length = bytes.length;
        for (int i = 0; i < bytes.length - 1; i++) {
            bytes[length - i - 1] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return bytes;
    }

    @Override
    public Long deserialize(byte[] bytes) {
        return deserialize(bytes, 0);
    }

    @Override
    public Long deserialize(byte[] bytes, int offset) {
        if (bytes[offset] == 0xF) return null;
        long value = 0;
        for (int index = offset; index < offset + descriptor().size(); index ++) {
            value = (value << 8) + (bytes[index] & 0xFF);
        }
        return value;
    }

    @Override
    public TypeDescriptor<Long> descriptor() {
        return null;
    }

}
