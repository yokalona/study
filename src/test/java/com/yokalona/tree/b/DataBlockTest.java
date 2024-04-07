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

    private record Datapoint(Integer key) implements WithKey<Integer> {

    }

}