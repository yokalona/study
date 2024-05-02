package com.yokalona.array.lazy.serializers;

public class LongSerializer implements Serializer<Long> {

    public static final Serializer<Long> INSTANCE = new LongSerializer();

    private LongSerializer() {}

    @Override
    public byte[] serialize(Long value) {
        byte[] bytes = new byte[Long.BYTES];
        int length = bytes.length;
        for (int i = 0; i < length; i++) {
            bytes[length - i - 1] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return bytes;
    }

    @Override
    public Long deserialize(byte[] bytes) {
        long value = 0;
        for (byte b : bytes) {
            value = (value << 8) + (b & 0xFF);
        }
        return value;
    }

    @Override
    public int sizeOf() {
        return Long.BYTES;
    }
}
