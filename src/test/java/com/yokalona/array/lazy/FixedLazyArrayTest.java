package com.yokalona.array.lazy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.yokalona.array.lazy.serializers.IntegerSerializer;
import com.yokalona.array.lazy.serializers.Serializer;
import com.yokalona.array.lazy.serializers.SerializerStorage;
import com.yokalona.tree.b.FileConfiguration;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class FixedLazyArrayTest {

    @Test
    public void testStore() {
        int size = 10_000_000;
        FixedLazyArray<INT> array = new FixedLazyArray<>(size, new FileConfiguration("fixed_test.la", "", null), INT.class);
        for (int i = 0; i < size; i ++) {
            array.set(i, new INT(i));
        }
        array.serialise();
        array = null;
        System.gc();

        FixedLazyArray<INT> loaded = FixedLazyArray.deserialize("fixed_test.la", INT.class);

        for (int i = 0; i < size; i ++) {
            assertEquals(i, loaded.get(i, false).value);
        }
    }

    @Test
    public void test() {
        int size = 10;
        INT[] second = new INT[size];
        for (int i = 0; i < size; i ++) {
            second[i] = new INT(i);
        }
        Kryo kryo = new Kryo();
        kryo.register(INT.class);
        kryo.register(INT[].class);
        try(Output output = new Output(new FileOutputStream("TTT.c"))) {
            kryo.writeObject(output, second);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    static class INT implements FixedSizeObject {

        private final Integer value;

        static {
            SerializerStorage.register(INT.class, new Serializer<>() {
                @Override
                public byte[] serialize(INT anInt) {
                    return IntegerSerializer.INSTANCE.serialize(anInt.value);
                }

                @Override
                public INT deserialize(byte[] value) {
                    return new INT(IntegerSerializer.INSTANCE.deserialize(value));
                }

                @Override
                public int sizeOf() {
                    return IntegerSerializer.INSTANCE.sizeOf();
                }
            });
        }

        INT(Integer value) {
            this.value = value;
        }

        public Integer
        get() {
            return value;
        }

        @Override
        public int sizeOf() {
            return Integer.BYTES;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            INT anInt = (INT) o;

            return Objects.equals(value, anInt.value);
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return Integer.toString(value);
        }
    }

}