package com.yokalona.tree.b;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class DataBlockTest {

    String fileName = "test-" + getClass().getSimpleName() + ".db";

    @Test
    public void testInsert() {
        DataBlock<Integer, Integer, Datapoint> dataBlock = new DataBlock<>(10, Datapoint.class);
        assertEquals(0, dataBlock.size());
        assertEquals(10, dataBlock.length());

        for (int sample = 0; sample < 10; sample++) {
            dataBlock.insert(new Datapoint(sample, sample, null));
            assertTrue(dataBlock.isOrdered());
            dataBlock.checkConsistency();
        }

        assertEquals(10, dataBlock.size());
        assertEquals(10, dataBlock.length());
    }

    @Test
    public void testFind() {
        DataBlock<Integer, Integer, Datapoint> dataBlock = new DataBlock<>(10, Datapoint.class);
        for (int sample = 0; sample < 10; sample++) dataBlock.insert(new Datapoint(sample, sample, null));
        assertEquals(10, dataBlock.size());
        assertTrue(dataBlock.isOrdered());
        for (int sample = 0; sample < 10; sample++) assertEquals(sample, dataBlock.equal(sample));
        for (int sample = 0; sample < 10; sample++) assertEquals(sample - 1, dataBlock.lessThan(sample));
        for (int sample = 0; sample < 10; sample++) assertEquals(sample + 1, dataBlock.greaterThan(sample));

        for (int sample = 10; sample < 20; sample++) assertEquals(- 11, dataBlock.equal(sample));
        for (int sample = - 1; sample > - 11; sample--) assertEquals(- 1, dataBlock.equal(sample));

        dataBlock.remove(5);
        assertEquals(5, dataBlock.equal(6));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLoad() throws FileNotFoundException {
        DataBlock<Integer, Integer, Datapoint> dataBlock = new DataBlock<>(10, Datapoint.class);
        dataBlock.insert(new Datapoint(0, 0, "0"));
        dataBlock.insert(new Datapoint(1, 1, "1"));
        dataBlock.insert(new Datapoint(2, 2, "2"));
        dataBlock.insert(new Datapoint(5, 5, "5"));

        Kryo kryo = new Kryo();
        kryo.register(DataBlock.class);
        kryo.register(Datapoint.class);
        kryo.register(Datapoint[].class);
        try (Output output = new Output(new FileOutputStream(fileName))) {
            kryo.writeObject(output, dataBlock);
        }

        assertNotNull(dataBlock);
        dataBlock = null;
        assertNull(dataBlock);
        System.gc();
        try (Input input = new Input(new FileInputStream(fileName))) {
            dataBlock = kryo.readObject(input, DataBlock.class);
            assertNotNull(dataBlock);
            dataBlock.check();
            assertEquals(0, dataBlock.get(0).key);
            assertEquals(1, dataBlock.get(1).key);
            assertEquals(2, dataBlock.get(2).key);
            assertEquals(5, dataBlock.get(3).key);
        }
    }

    private record Datapoint(Integer key, Integer value, Object link)
            implements HasKey<Integer>, HasValue<Integer>, HasLink<Object> {
    }

}