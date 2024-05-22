package com.yokalona.tree.b;

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
        Loader<Integer, Integer> loader = new Loader<>(fileName, 10);
        DataBlock<Integer, Integer> dataBlock = new DataBlock<>(10, true);
        assertEquals(0, dataBlock.size());
        assertEquals(10, dataBlock.length());

        for (int sample = 0; sample < 10; sample++) {
            dataBlock.insert(sample, sample, sample);
            assertTrue(dataBlock.isOrdered());
            dataBlock.checkConsistency();
        }

        assertEquals(10, dataBlock.size());
        assertEquals(10, dataBlock.length());
    }

    @Test
    public void testFind() {
        Loader<Integer, Integer> loader = new Loader<>(fileName, 10);
        DataBlock<Integer, Integer> dataBlock = new DataBlock<>(10, true);
        for (int sample = 0; sample < 10; sample++) dataBlock.insert(sample, sample, sample);
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

}