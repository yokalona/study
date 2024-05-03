package com.yokalona.array.lazy.serializers;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class IntegerSerializerTest {

    @Test
    public void testSerialize() {
        assertArrayEquals(new byte[5], IntegerSerializer.get.serialize(0));
        assertArrayEquals(new byte[] {0, 0, 0, 0, 1}, IntegerSerializer.get.serialize(1));
        assertArrayEquals(new byte[] {0, 0, 0, 0, 0x10}, IntegerSerializer.get.serialize(16));
        assertArrayEquals(new byte[] {0, 0x7F, -0x01, -0x01, -0x01}, IntegerSerializer.get.serialize(Integer.MAX_VALUE));
        assertArrayEquals(new byte[] {0, -0x80, 0, 0, 0}, IntegerSerializer.get.serialize(Integer.MIN_VALUE));
        assertArrayEquals(new byte[] {0xF, 0x0, 0x0, 0x0, 0x0}, IntegerSerializer.get.serialize(null));
    }

    @Test
    public void testDeserialize() {
        assertEquals(0, IntegerSerializer.get.deserialize(new byte[5]));
        assertEquals(1, IntegerSerializer.get.deserialize(new byte[] {0, 0, 0, 0, 1}));
        assertEquals(16, IntegerSerializer.get.deserialize(new byte[] {0, 0, 0, 0, 0x10}));
        assertEquals(Integer.MAX_VALUE, IntegerSerializer.get.deserialize(new byte[] {0, 0x7F, -0x01, -0x01, -0x01}));
        assertEquals(Integer.MIN_VALUE, IntegerSerializer.get.deserialize(new byte[] {0, -0x80, 0, 0, 0}));
        assertNull(IntegerSerializer.get.deserialize(new byte[] {0xF}));
    }

    @Test
    public void testDeserializeWithOffset() {
        assertEquals(0, IntegerSerializer.get.deserialize(new byte[6], 1));
        assertEquals(1, IntegerSerializer.get.deserialize(new byte[] {0, 0, 0, 0, 0, 1}, 1));
        assertEquals(16, IntegerSerializer.get.deserialize(new byte[] {0xF, 0xF, 0, 0, 0, 0, 0x10}, 2));
        assertEquals(Integer.MAX_VALUE, IntegerSerializer.get.deserialize(new byte[] {1, 2, 3, 4, 0, 0x7F, -0x01, -0x01, -0x01}, 4));
        assertEquals(Integer.MIN_VALUE, IntegerSerializer.get.deserialize(new byte[] {0, -0x80, 0, 0, 0}));
        assertNull(IntegerSerializer.get.deserialize(new byte[] {0xF}));
    }

}