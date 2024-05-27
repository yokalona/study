package com.yokalona.array.persitent.serializers;

public record IntegerSerializer(TypeDescriptor<Integer> descriptor, byte[] bytes) implements Serializer<Integer> {

    public IntegerSerializer(TypeDescriptor<Integer> descriptor) {
        this(descriptor, new byte[descriptor.size()]);
    }

    @Override
    public byte[] serialize(Integer value) {
        if (value == null) {
            bytes[0] = 0xF;
            return bytes;
        } else bytes[0] = 0x0;
        int length = bytes.length;
        int vals = value;
        for (int i = 0; i < bytes.length - 1; i++) {
            bytes[length - i - 1] = (byte) (vals & 0xFF);
            vals >>= 8;
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
