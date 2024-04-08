package com.yokalona.tree.b;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataBlockTest {

    @Test
    public void testInsert() {
        DataBlock<Integer, Datapoint> dataBlock = new DataBlock<>(() -> new Datapoint[10]);
        assertEquals(0, dataBlock.size());
        assertEquals(10, dataBlock.length());

        for (int sample = 0; sample < 10; sample ++) {
            dataBlock.insert(new Datapoint(sample));
            assertTrue(dataBlock.isOrdered());
            dataBlock.checkConsistency();
        }

        assertEquals(10, dataBlock.size());
        assertEquals(10, dataBlock.length());
    }

    @Test
    public void testFind() {
        DataBlock<Integer, Datapoint> dataBlock = new DataBlock<>(() -> new Datapoint[10]);
        for (int sample = 0; sample < 10; sample ++) dataBlock.insert(new Datapoint(sample));
        assertEquals(10, dataBlock.size());
        assertTrue(dataBlock.isOrdered());
        for (int sample = 0; sample < 10; sample ++) assertEquals(sample, dataBlock.find.equal(sample));
        for (int sample = 0; sample < 10; sample ++) assertEquals(sample - 1, dataBlock.find.lessThan(sample));
        for (int sample = 0; sample < 10; sample ++) assertEquals(sample + 1, dataBlock.find.greaterThan(sample));

        for (int sample = 10; sample < 20; sample ++) assertEquals(-10, dataBlock.find.position(sample));
        for (int sample = -1; sample > -11; sample --) assertEquals(0, dataBlock.find.position(sample));

        dataBlock.remove(5);
        assertEquals(5, dataBlock.find.position(6));


    }

    private record Datapoint(Integer key) implements WithKey<Integer> {

    }

}