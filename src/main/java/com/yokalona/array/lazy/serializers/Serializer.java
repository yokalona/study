package com.yokalona.array.lazy.serializers;

public interface Serializer<Type> {

    byte[] serialize(Type type);
    Type deserialize(byte[] bytes);
    int sizeOf();

}
