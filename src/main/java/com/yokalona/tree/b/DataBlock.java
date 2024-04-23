package com.yokalona.tree.b;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

import static com.yokalona.Validations.CAPACITY_SHOULD_BE_GREATER_THAN_2;
import static com.yokalona.Validations.KEY_SHOULD_HAVE_NON_NULL_VALUE;

public class DataBlock<Key extends Comparable<Key>, Value>
        implements Iterable<Leaf<Key, Value>> {

    public static boolean ENABLE_CHECK = false;

    private int size;
    private final Leaf<Key, Value>[] array;

    @SuppressWarnings("unchecked")
    public DataBlock(int capacity) {
        assert capacity > 2 : CAPACITY_SHOULD_BE_GREATER_THAN_2;

        this.array = (Leaf<Key, Value>[]) Array.newInstance(Leaf.class, capacity);
    }

    public Leaf<Key, Value>
    get(int index) {
        return array[index];
    }

    public Key
    getMaxKey() {
        return array[size - 1].key();
    }

    public Key
    getMinKey() {
        return array[0].key();
    }

    public Node<Key, Value>
    getMaxLink() {
        return array[size - 1].link();
    }

    public Node<Key, Value>
    getMinLink() {
        return array[0].link();
    }

    public Entry<Key, Value>
    getMaxEntry() {
        return Entry.fromLeaf(array[size - 1]);
    }

    public Entry<Key, Value>
    getMinEntry() {
        return Entry.fromLeaf(array[0]);
    }

    public boolean
    contains(int index) {
        return array[index] != null;
    }

    public Key
    getKey(int index) {
        return array[index].key();
    }

    public Value
    getValue(int index) {
        return array[index].value();
    }

    public Node<Key, Value>
    getLink(int index) {
        return array[index].link();
    }

    public void
    replaceInternal(int index, Key key, Node<Key, Value> link) {
        this.array[index] = Leaf.internal(key, link);
        assert check();
    }

    public void
    replaceExternal(int index, Key key, Value value) {
        this.array[index] = Leaf.external(key, value);
        assert check();
    }

    public void
    insert(Leaf<Key, Value> value) {
        assert value != null : "Null values are not permitted";

        array[size++] = value;
        assert check();
    }

    public void
    insertMin(DataBlock<Key, Value> dataBlock) {
//        assert value != null : "Null values are not permitted";

        array[size++] = dataBlock.get(0);
        assert check();
    }

    public void
    insertExternal(int index, Key key, Value value) {
        assert value != null : "Null values are not permitted";

        System.arraycopy(array, index, array, index + 1, size - index);
        array[index] = Leaf.external(key, value);
        size++;

        assert check();
    }

    public void
    insertInternal(int index, Key key, Node<Key, Value> link) {
        assert link != null : "Null link is not permitted";

        System.arraycopy(array, index, array, index + 1, size - index);
        array[index] = Leaf.internal(key, link);
        size++;

        assert check();
    }

    public void
    insertMax(int index, DataBlock<Key, Value> value) {
        assert value != null : "Null values are not permitted";

        System.arraycopy(array, index, array, index + 1, size - index);
        array[index] = value.get(value.size - 1);
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
    copyTo(DataBlock<Key, Value> other, int from, int to, int length) {
        System.arraycopy(array, from, other.array, to, length);
        other.size += length;

        assert check();
        assert other.check();
    }

    public void
    splitWith(DataBlock<Key, Value> other) {
        copyTo(other, array.length / 2, 0, array.length / 2);
        remove(array.length / 2, array.length);

        assert check();
        assert other.check();
    }

    public void
    mergeWith(DataBlock<Key, Value> other) {
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
    public Iterator<Leaf<Key, Value>>
    iterator() {
        return Arrays.stream(this.array, 0, size).iterator();
    }

    @Override
    public void
    forEach(Consumer<? super Leaf<Key, Value>> action) {
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<Leaf<Key, Value>>
    spliterator() {
        return Arrays.spliterator(array);
    }

    boolean
    check() {
        assert ! ENABLE_CHECK || (checkConsistency() && isOrdered());
        return true;
    }

    boolean
    checkConsistency() {
        assert size >= 0;
        assert size <= array.length;
        for (int point = 0; point < size; point++) if (array[point] == null) return false;
        for (int point = size; point < array.length; point++) if (array[point] != null) return false;
        return true;
    }

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
