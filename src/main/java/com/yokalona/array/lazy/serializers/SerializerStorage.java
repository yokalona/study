package com.yokalona.array.lazy.serializers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SerializerStorage {

    private static final HashMap<TypeDescriptor<?>, Serializer<?>> map = new HashMap<>();
    public static final Serializer<Integer> INTEGER;

    static {
        TypeDescriptor<Integer> integerType = new TypeDescriptor<>(Integer.BYTES + 1, Integer.class);
        register(integerType, INTEGER = new IntegerSerializer(integerType));
    }

    public static <Type> void
    register(TypeDescriptor<Type> type, Serializer<Type> serializer) {
        map.put(type, serializer);
    }

    @SuppressWarnings("unchecked")
    public static <Type> Serializer<Type>
    get(TypeDescriptor<Type> type) {
        return (Serializer<Type>) map.get(type);
    }

}
