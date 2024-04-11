package com.yokalona.tree.b;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataBlockTest {

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
    public void testLoad() throws FileNotFoundException {
//        Kryo kryo = new Kryo();
//        kryo.register(DataBlock.class);
//        kryo.register(DataBlock.class);
//        kryo.register(Datapoint.class);
//        kryo.register(Datapoint[].class);
        DataBlock<Integer, Integer, Datapoint> dataBlock = new DataBlock<>(10, Datapoint.class);
//        dataBlock.insert(new Datapoint(0, 0, "0"));
//        dataBlock.insert(new Datapoint(1, 1, "1"));
//        dataBlock.insert(new Datapoint(2, 2, "2"));

        dataBlock.unload();
        System.out.println(dataBlock);
        dataBlock.load();
        System.out.println(dataBlock);

//        Output output = new Output(new FileOutputStream("file.bin"));
//        kryo.writeObject(output, dataBlock);
//        output.close();
//
//        Input input = new Input(new FileInputStream("file.bin"));
//        @SuppressWarnings("unchecked")
//        DataBlock<Integer, Integer, Datapoint> object2 = (DataBlock<Integer, Integer, Datapoint>) kryo.readObject(input, DataBlock.class);
//        input.close();
//        System.out.println(object2);
    }

    private record Datapoint(Integer key, Integer value, Object link)
            implements HasKey<Integer>, HasValue<Integer>, HasLink<Object> {
    }

}