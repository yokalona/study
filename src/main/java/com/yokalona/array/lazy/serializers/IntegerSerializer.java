package com.yokalona.array.lazy.serializers;

public record IntegerSerializer(TypeDescriptor<Integer> descriptor) implements Serializer<Integer> {

    @Override
    public byte[] serialize(Integer value) {
        byte[] bytes = new byte[descriptor.size()];
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
    public Integer deserialize(byte[] bytes) {
        return deserialize(bytes, 0);
    }

    @Override
    public Integer deserialize(byte[] bytes, int offset) {
        if (bytes[offset] == 0xF) return null;
        int value = 0;
        for (int index = offset + 1; index < offset + descriptor.size(); index ++) {
            value = (value << 8) + (bytes[index] & 0xFF);
        }
        return value;
    }

}
