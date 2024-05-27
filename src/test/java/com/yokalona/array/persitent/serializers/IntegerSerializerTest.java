package com.yokalona.array.persitent.serializers;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IntegerSerializerTest {

    @Test
    public void testSerialize() {
        assertArrayEquals(new byte[5], SerializerStorage.INTEGER.serialize(0));
        assertArrayEquals(new byte[] {0, 0, 0, 0, 1}, SerializerStorage.INTEGER.serialize(1));
        assertArrayEquals(new byte[] {0, 0, 0, 0, 0x10}, SerializerStorage.INTEGER.serialize(16));
        assertArrayEquals(new byte[] {0, 0x7F, -0x01, -0x01, -0x01}, SerializerStorage.INTEGER.serialize(Integer.MAX_VALUE));
        assertArrayEquals(new byte[] {0, -0x80, 0, 0, 0}, SerializerStorage.INTEGER.serialize(Integer.MIN_VALUE));
        assertEquals(0xF, SerializerStorage.INTEGER.serialize(null)[0]);
    }

    @Test
    public void testDeserialize() {
        assertEquals(0, SerializerStorage.INTEGER.deserialize(new byte[5]));
        assertEquals(1, SerializerStorage.INTEGER.deserialize(new byte[] {0, 0, 0, 0, 1}));
        assertEquals(16, SerializerStorage.INTEGER.deserialize(new byte[] {0, 0, 0, 0, 0x10}));
        assertEquals(Integer.MAX_VALUE, SerializerStorage.INTEGER.deserialize(new byte[] {0, 0x7F, -0x01, -0x01, -0x01}));
        assertEquals(Integer.MIN_VALUE, SerializerStorage.INTEGER.deserialize(new byte[] {0, -0x80, 0, 0, 0}));
        assertNull(SerializerStorage.INTEGER.deserialize(new byte[] {0xF}));
    }

    @Test
    public void testDeserializeWithOffset() {
        assertEquals(0, SerializerStorage.INTEGER.deserialize(new byte[6], 1));
        assertEquals(1, SerializerStorage.INTEGER.deserialize(new byte[] {0, 0, 0, 0, 0, 1}, 1));
        assertEquals(16, SerializerStorage.INTEGER.deserialize(new byte[] {0xF, 0xF, 0, 0, 0, 0, 0x10}, 2));
        assertEquals(Integer.MAX_VALUE, SerializerStorage.INTEGER.deserialize(new byte[] {1, 2, 3, 4, 0, 0x7F, -0x01, -0x01, -0x01}, 4));
        assertEquals(Integer.MIN_VALUE, SerializerStorage.INTEGER.deserialize(new byte[] {0, -0x80, 0, 0, 0}));
        assertNull(SerializerStorage.INTEGER.deserialize(new byte[] {0xF}));
    }

}