package com.yokalona.tree.b;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

import static com.yokalona.Validations.CAPACITY_SHOULD_BE_GREATER_THAN_2;
import static com.yokalona.Validations.KEY_SHOULD_HAVE_NON_NULL_VALUE;

public class DataBlock<Key extends Comparable<Key>, Value, Data extends HasKey<? extends Key> & HasValue<? extends Value> & HasLink<?>>
        implements Iterable<Data>, KryoSerializable {

    private int size;
    private Data[] array;

    private DataBlock() {}

    @SuppressWarnings("unchecked")
    public DataBlock(int capacity, Class<Data> dataClass) {
        assert dataClass != null;
        assert !dataClass.isArray();
        assert capacity > 2 : CAPACITY_SHOULD_BE_GREATER_THAN_2;

        this.array = (Data[]) Array.newInstance(dataClass, capacity);
    }

    @Override
    public void
    write(Kryo kryo, Output output) {
        kryo.writeClass(output, array.getClass());
        kryo.writeObject(output, array);
        kryo.writeObject(output, size);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void
    read(Kryo kryo, Input input) {
        Registration arrayClass = kryo.readClass(input);
        this.array = (Data[]) kryo.readObject(input, arrayClass.getType());
        this.size = kryo.readObject(input, int.class);
    }

    public Data
    get(int index) {
        return array[index];
    }

    public void
    replace(int index, Data value) {
        assert value != null : "Null values are not permitted";

        this.array[index] = value;
        assert check();
    }

    public void
    insert(Data value) {
        assert value != null : "Null values are not permitted";

        array[size++] = value;
        assert check();
    }

    public void
    insert(int index, Data value) {
        assert value != null : "Null values are not permitted";

        System.arraycopy(array, index, array, index + 1, size - index);
        array[index] = value;
        size++;

        assert check();
    }

    public void
    remove(int index) {
        System.arraycopy(array, index + 1, array, index, size - index - 1);
        array[-- size] = null;

        assert check();
    }

    private void
    copyTo(DataBlock<Key, Value, Data> other, int from, int to, int length) {
        System.arraycopy(array, from, other.array, to, length);
        other.size += length;

        assert check();
        assert other.check();
    }

    public void
    splitWith(DataBlock<Key, Value, Data> other) {
        copyTo(other, array.length / 2, 0, array.length / 2);
        remove(array.length / 2, array.length);

        assert check();
        assert other.check();
    }

    public void
    mergeWith(DataBlock<Key, Value, Data> other) {
        copyTo(other, 0, other.size, size);

        assert check();
        assert other.check();
    }

    public void
    remove(int from, int to) {
        Arrays.fill(array, from, to, null);
        size -= to - from;

        assert check();
    }

    public int
    length() {
        return this.array.length;
    }

    public int
    size() {
        return this.size;
    }

    @Override
    public Iterator<Data>
    iterator() {
        return Arrays.stream(this.array, 0, size).iterator();
    }

    @Override
    public void
    forEach(Consumer<? super Data> action) {
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<Data>
    spliterator() {
        return Arrays.spliterator(array);
    }

    boolean
    check() {
        checkConsistency();
        assert isOrdered();
        return true;
    }

    void
    checkConsistency() {
        assert size >= 0;
        assert size <= array.length;
        for (int point = 0; point < size; point++) assert array[point] != null;
        for (int point = size; point < array.length; point++) assert array[point] == null;
    }

    @SuppressWarnings("unchecked")
    boolean
    isOrdered() {
        Key previous = null;
        for (int point = 0; point < size; point++) {
            if (previous == null || array[point].key().compareTo(previous) > 0) previous = array[point].key();
            else return false;
        }
        return true;
    }

    public int
    equal(final Key key) {
        assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

        int left = 0, right = size - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            int comparison = array[mid].key().compareTo(key);
            if (comparison > 0) right = mid - 1;
            else if (comparison < 0) left = mid + 1;
            else return mid;
        }
        return - (left + 1);
    }

    public int
    lessThan(final Key key) {
        int equal = equal(key);
        if (equal >= 0) return equal - 1;
        else return Math.max(- equal - 2, 0);
    }

    public int
    greaterThan(final Key key) {
        int equal = equal(key);
        if (equal >= 0) return equal + 1;
        else return Math.max(- equal - 1, 1);
    }

    public String
    toString() {
        return "[" + size + ':' + array.length + ']' + ' ' + appendArray();
    }

    private String
    appendArray() {
        if (size == 0) return "[]";
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        int iterations = Math.min(size, 10);
        for (int i = 0; i < iterations; i++) {
            sb.append(array[i]);
            if (i < iterations - 1) sb.append('\n').append('\t').append('\t');
        }
        if (size > 10) {
            sb.repeat(".", 3);
        }
        return sb.append(']').toString();
    }

}
