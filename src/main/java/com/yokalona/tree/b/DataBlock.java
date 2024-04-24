package com.yokalona.tree.b;

import java.util.*;

import static com.yokalona.Validations.*;

public class DataBlock<Key extends Comparable<Key>, Value> {
    public static boolean ENABLE_CHECK = false;

    private final Object[] keys;
    private final Object[] values;
    private final boolean leaf;

    private int size;

    public DataBlock(int capacity, boolean leaf) {
        assert capacity > 2 : CAPACITY_SHOULD_BE_GREATER_THAN_2;

        this.leaf = leaf;
        this.keys = new Object[capacity];
        this.values = new Object[capacity];
    }

    @SuppressWarnings("unchecked")
    public Key
    key(int index) {
        return (Key) keys[index];
    }

    public Key
    minKey() {
        return key(0);
    }

    public Key
    maxKey() {
        return key(size - 1);
    }

    @SuppressWarnings("unchecked")
    public Node<Key, Value>
    link(int index) {
        assert !leaf;

        return (Node<Key, Value>) values[index];
    }

    public Node<Key, Value>
    minLink() {
        return leaf ? null : link(0);
    }

    public Node<Key, Value>
    maxLink() {
        return leaf ? null : link(size - 1);
    }

    public Entry<Key, Value>
    entry(int index) {
        assert leaf;

        return new Entry<>(key(index), value(index));
    }

    public Entry<Key, Value>
    minEntry() {
        return entry(0);
    }

    public Entry<Key, Value>
    maxEntry() {
        return entry(size - 1);
    }

    @SuppressWarnings("unchecked")
    public Value
    value(int index) {
        assert leaf;

        return (Value) values[index];
    }

    public Value
    minValue() {
        return value(0);
    }

    public Value
    maxValue() {
        return value(size - 1);
    }

    public boolean
    contains(int index) {
        return keys[index] != null;
    }

    private void
    insertKey(int index, Key key) {
        assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

        System.arraycopy(keys, index, keys, index + 1, size - index);
        keys[index] = key;
    }

    public void
    insertExternal(int index, Key key, Value value) {
        insertKey(index, key);
        System.arraycopy(values, index, values, index + 1, size - index);
        values[index] = value;
        size++;

        assert check();
    }

    public void
    insertInternal(int index, Key key, Node<Key, Value> link) {
        assert link != null : NULL_LINK_IS_NOT_PERMITTED;

        insertKey(index, key);
        System.arraycopy(values, index, values, index + 1, size - index);
        assert !leaf;

        values[index] = link;
        size++;

        assert check();
    }

    public void
    insertMinFrom(DataBlock<Key, Value> datablock) {
        assert datablock != null : DATA_BLOCK_CANNOT_BE_NULL;

        if (leaf) insertExternal(size, datablock.minKey(), datablock.minValue());
        else insertInternal(size, datablock.minKey(), datablock.minLink());

        assert check();
    }

    public void
    insertMaxFrom(int index, DataBlock<Key, Value> datablock) {
        assert datablock != null : DATA_BLOCK_CANNOT_BE_NULL;

        if (leaf) insertExternal(index, datablock.maxKey(), datablock.maxValue());
        else insertInternal(index, datablock.maxKey(), datablock.maxLink());

        assert check();
    }

    public void
    replaceInternal(int index, Key key, Node<Key, Value> link) {
        assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
        assert link != null : NULL_LINK_IS_NOT_PERMITTED;

        this.keys[index] = key;
        assert !leaf;

        values[index] = link;

        assert check();
    }

    public void
    replaceExternal(int index, Key key, Value value) {
        assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

        this.keys[index] = key;
        this.values[index] = value;

        assert check();
    }

    public void
    remove(int index) {
        System.arraycopy(keys, index + 1, keys, index, size - index - 1);
        System.arraycopy(values, index + 1, values, index, size - index - 1);
        values[size - 1] = null;
        keys[-- size] = null;

        assert check();
    }

    public void
    remove(int from, int to) {
        Arrays.fill(keys, from, to, null);
        Arrays.fill(values, from, to, null);
        size -= to - from;

        assert check();
    }

    private void
    copyTo(DataBlock<Key, Value> other, int from, int to, int length) {
        System.arraycopy(keys, from, other.keys, to, length);
        System.arraycopy(values, from, other.values, to, length);
        other.size += length;

        assert check();
        assert other.check();
    }

    public void
    splitWith(DataBlock<Key, Value> other) {
        copyTo(other, keys.length / 2, 0, keys.length / 2);
        remove(keys.length / 2, keys.length);

        assert check();
        assert other.check();
    }

    public void
    mergeWith(DataBlock<Key, Value> other) {
        copyTo(other, 0, other.size, size);

        assert check();
        assert other.check();
    }

    public int
    length() {
        return this.keys.length;
    }

    public int
    size() {
        return this.size;
    }

    boolean
    check() {
        assert ! ENABLE_CHECK || (checkConsistency() && isOrdered());
        return true;
    }

    boolean
    checkConsistency() {
        assert size >= 0;
        assert size <= keys.length;
        for (int point = 0; point < size; point++) assert keys[point] != null;
        for (int point = size; point < keys.length; point++) assert keys[point] == null;
        return true;
    }

    boolean
    isOrdered() {
        Key previous = null;
        for (int point = 0; point < size; point++) {
            if (previous == null || key(point).compareTo(previous) > 0) previous = key(point);
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
            int comparison = key(mid).compareTo(key);
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
        return "[" + size + ':' + keys.length + ']' + ' ' + appendArray();
    }

    private String
    appendArray() {
        if (size == 0) return "[]";
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        int iterations = Math.min(size, 10);
        for (int i = 0; i < iterations; i++) {
            sb.append(keys[i]);
            if (i < iterations - 1) sb.append('\n').append('\t').append('\t');
        }
        if (size > 10) {
            sb.repeat(".", 3);
        }
        return sb.append(']').toString();
    }

}
