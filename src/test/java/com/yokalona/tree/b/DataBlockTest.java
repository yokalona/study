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
        DataBlock<Integer, Integer> dataBlock = new DataBlock<>(10);
        assertEquals(0, dataBlock.size());
        assertEquals(10, dataBlock.length());

        for (int sample = 0; sample < 10; sample++) {
            dataBlock.insert(new Leaf<>(sample, sample, null, true));
            assertTrue(dataBlock.isOrdered());
            dataBlock.checkConsistency();
        }

        assertEquals(10, dataBlock.size());
        assertEquals(10, dataBlock.length());
    }

    @Test
    public void testFind() {
        DataBlock<Integer, Integer> dataBlock = new DataBlock<>(10);
        for (int sample = 0; sample < 10; sample++) dataBlock.insert(new Leaf<>(sample, sample, null, true));
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
        DataBlock<Integer, Integer> dataBlock = new DataBlock<>(10);
        dataBlock.insert(new Leaf<>(0, 0, new Node<>(10), false));
        dataBlock.insert(new Leaf<>(1, 1, new Node<>(10), false));
        dataBlock.insert(new Leaf<>(2, 2, new Node<>(10), false));
        dataBlock.insert(new Leaf<>(5, 5, new Node<>(10), false));

        Loader<Integer, Integer> loader = new Loader<>("test", 10);
        try (Output output = new Output(new FileOutputStream(fileName))) {
            loader.kryo().writeObject(output, dataBlock);
        }

        assertNotNull(dataBlock);
        dataBlock = null;
        System.gc();
        try (Input input = new Input(new FileInputStream(fileName))) {
            dataBlock = loader.kryo().readObject(input, DataBlock.class);
            assertNotNull(dataBlock);
            dataBlock.check();
            assertEquals(0, dataBlock.get(0).key());
            assertEquals(1, dataBlock.get(1).key());
            assertEquals(2, dataBlock.get(2).key());
            assertEquals(5, dataBlock.get(3).key());
        }
    }

}