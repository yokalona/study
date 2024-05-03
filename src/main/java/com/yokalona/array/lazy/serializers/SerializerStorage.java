package com.yokalona.array.lazy.serializers;

import java.util.HashMap;

public class SerializerStorage {

    private static final HashMap<Class<?>, Serializer<?>> map = new HashMap<>() {{
        put(Integer.class, IntegerSerializer.get);
        put(Long.class, LongSerializer.INSTANCE);
    }};

    public static <Type> void
    register(Class<Type> type, Serializer<Type> serializer) {
        map.put(type, serializer);
    }

    @SuppressWarnings("unchecked")
    public static <Type> Serializer<Type>
    get(Class<Type> type) {
        return (Serializer<Type>) map.get(type);
    }

}
