package com.yokalona.array.lazy.serializers;

public class IntegerSerializer implements Serializer<Integer> {

    public static final Serializer<Integer> INSTANCE = new IntegerSerializer();

    private IntegerSerializer() {}

    @Override
    public byte[] serialize(Integer value) {
        byte[] bytes = new byte[Integer.BYTES];
        int length = bytes.length;
        for (int i = 0; i < length; i++) {
            bytes[length - i - 1] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return bytes;
    }

    @Override
    public Integer deserialize(byte[] bytes) {
        int value = 0;
        for (byte b : bytes) {
            value = (value << 8) + (b & 0xFF);
        }
        return value;
    }

    @Override
    public int sizeOf() {
        return Integer.BYTES;
    }
}
