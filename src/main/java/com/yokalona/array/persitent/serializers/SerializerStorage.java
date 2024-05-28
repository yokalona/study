package com.yokalona.array.persitent.serializers;

import java.util.HashMap;

public class SerializerStorage {
    private static final HashMap<TypeDescriptor<?>, Serializer<?>> map = new HashMap<>();
    public static final Serializer<Integer> INTEGER;
    public static final Serializer<Boolean> BOOLEAN;

    static {
        TypeDescriptor<Integer> integerType = new TypeDescriptor<>(Integer.BYTES + 1, Integer.class);
        TypeDescriptor<Boolean> booleanType = new TypeDescriptor<>(1, Boolean.class);
        register(integerType, INTEGER = new IntegerSerializer(integerType));
        register(booleanType, BOOLEAN = new BooleanSerializer(booleanType));
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
