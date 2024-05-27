package com.yokalona.array.persitent.serializers;

public record BooleanSerializer(TypeDescriptor<Boolean> descriptor, byte[] bytes) implements Serializer<Boolean> {

    public BooleanSerializer(TypeDescriptor<Boolean> descriptor) {
        this(descriptor, new byte[descriptor.size()]);
    }

    @Override
    public byte[]
    serialize(Boolean value) {
        if (value == null) bytes[0] = 0xF;
        else if (value) bytes[0] = 0x1;
        else bytes[0] = 0x0;
        return bytes;
    }

    @Override
    public Boolean
    deserialize(byte[] bytes) {
        return deserialize(bytes, 0);
    }

    @Override
    public Boolean
    deserialize(byte[] bytes, int offset) {
        return switch (bytes[offset]) {
            case 0x0 -> false;
            case 0x1 -> true;
            default -> null;
        };
    }

}
