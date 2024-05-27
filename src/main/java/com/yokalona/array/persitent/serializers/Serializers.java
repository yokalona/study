package com.yokalona.array.persitent.serializers;

public class Serializers {

    public static byte[]
    serialize(int value) {
        return SerializerStorage.INTEGER.serialize(value);
    }

    public static byte[]
    serialize(boolean value) {
        return SerializerStorage.BOOLEAN.serialize(value);
    }

    @SuppressWarnings("unchecked")
    public static <Type> byte[]
    serialize(TypeDescriptor<Type> descriptor, Object value) {
        return SerializerStorage.get(descriptor).serialize((Type) value);
    }

    public static int
    deserialize(byte[] bytes, int ignore) {
        return deserialize(bytes, 0, ignore);
    }

    public static int
    deserialize(byte[] bytes, int offset, int ignore) {
        return SerializerStorage.INTEGER.deserialize(bytes, offset);
    }

    public static boolean
    deserialize(byte[] bytes, boolean ignore) {
        return deserialize(bytes, 0, ignore);
    }

    public static boolean
    deserialize(byte[] bytes, int offset, boolean ignore) {
        return SerializerStorage.BOOLEAN.deserialize(bytes, offset);
    }

    public static <Type> Type
    deserialize(TypeDescriptor<Type> descriptor, byte[] bytes) {
        return SerializerStorage.get(descriptor).deserialize(bytes);
    }

    public static int
    typeSize(int ignore) {
        return SerializerStorage.INTEGER.descriptor().size();
    }

    public static int
    typeSize(boolean ignore) {
        return SerializerStorage.BOOLEAN.descriptor().size();
    }

}
