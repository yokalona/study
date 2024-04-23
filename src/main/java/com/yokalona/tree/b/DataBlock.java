package com.yokalona.tree.b;

import java.util.*;
import java.util.function.Consumer;

import static com.yokalona.Validations.*;

public class DataBlock<Key extends Comparable<Key>, Value>
        implements Iterable<Leaf<Key, Value>> {
    public static boolean ENABLE_CHECK = false;

    private int size;

    private final Object[] keys;
    private final Object[] values;
    private final Node<Key, Value>[] links;

    @SuppressWarnings("unchecked")
    public DataBlock(int capacity) {
        assert capacity > 2 : CAPACITY_SHOULD_BE_GREATER_THAN_2;

        this.keys = new Object[capacity];
        this.values = new Object[capacity];
        this.links = (Node<Key, Value>[]) new Node[capacity];
    }

    @SuppressWarnings("unchecked")
    public Key
    getKey(int index) {
        return (Key) keys[index];
    }

    public Key
    getMinKey() {
        return getKey(0);
    }

    public Key
    getMaxKey() {
        return getKey(size - 1);
    }

    public Node<Key, Value>
    getLink(int index) {
        return links[index];
    }

    public Node<Key, Value>
    getMinLink() {
        return getLink(0);
    }

    public Node<Key, Value>
    getMaxLink() {
        return getLink(size - 1);
    }

    public Entry<Key, Value>
    getEntry(int index) {
        return new Entry<>(getKey(index), getValue(index));
    }

    public Entry<Key, Value>
    getMinEntry() {
        return getEntry(0);
    }

    public Entry<Key, Value>
    getMaxEntry() {
        return getEntry(size - 1);
    }

    @SuppressWarnings("unchecked")
    public Value
    getValue(int index) {
        return (Value) values[index];
    }

    public Value
    getMinValue() {
        return getValue(0);
    }

    public Value
    getMaxValue() {
        return getValue(size - 1);
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
        assert value != null : "Null values are not permitted";

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
        System.arraycopy(links, index, links, index + 1, size - index);
        links[index] = link;
        size++;

        assert check();
    }

    public void
    insertMinFrom(DataBlock<Key, Value> datablock, boolean leaf) {
        assert datablock != null : DATA_BLOCK_CANNOT_BE_NULL;

        if (leaf) insertExternal(size, datablock.getMinKey(), datablock.getMinValue());
        else insertInternal(size, datablock.getMinKey(), datablock.getMinLink());

        assert check();
    }

    public void
    insertMaxFrom(int index, DataBlock<Key, Value> datablock, boolean leaf) {
        assert datablock != null : DATA_BLOCK_CANNOT_BE_NULL;

        if (leaf) insertExternal(index, datablock.getMaxKey(), datablock.getMaxValue());
        else insertInternal(index, datablock.getMaxKey(), datablock.getMaxLink());

        assert check();
    }

    public void
    replaceInternal(int index, Key key, Node<Key, Value> link) {
        assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
        assert link != null : NULL_LINK_IS_NOT_PERMITTED;

        this.keys[index] = key;
        this.links[index] = link;

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
    remove(int index, boolean leaf) {
        System.arraycopy(keys, index + 1, keys, index, size - index - 1);
        if (leaf) {
            System.arraycopy(values, index + 1, values, index, size - index - 1);
            values[size - 1] = null;
        } else {
            System.arraycopy(links, index + 1, links, index, size - index - 1);
            links[size - 1] = null;
        }
        keys[-- size] = null;

        assert check();
    }

    public void
    remove(int from, int to, boolean leaf) {
        Arrays.fill(keys, from, to, null);
        if (leaf) Arrays.fill(values, from, to, null);
        else Arrays.fill(links, from, to, null);
        size -= to - from;

        assert check();
    }

    private void
    copyTo(DataBlock<Key, Value> other, int from, int to, int length, boolean leaf) {
        System.arraycopy(keys, from, other.keys, to, length);
        if (leaf) System.arraycopy(values, from, other.values, to, length);
        else System.arraycopy(links, from, other.links, to, length);
        other.size += length;

        assert check();
        assert other.check();
    }

    public void
    splitWith(DataBlock<Key, Value> other, boolean leaf) {
        copyTo(other, keys.length / 2, 0, keys.length / 2, leaf);
        remove(keys.length / 2, keys.length, leaf);

        assert check();
        assert other.check();
    }

    public void
    mergeWith(DataBlock<Key, Value> other, boolean leaf) {
        copyTo(other, 0, other.size, size, leaf);

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

    private List<Leaf<Key, Value>> formList() {
        List<Leaf<Key, Value>> list = new ArrayList<>();
        for (int leaf = 0; leaf < size; leaf++) {
            list.add(new Leaf<>(getKey(leaf), getValue(leaf), getLink(leaf)));
        }
        return list;
    }

    @Override
    public Iterator<Leaf<Key, Value>>
    iterator() {
        List<Leaf<Key, Value>> list = formList();
        return list.iterator();
    }

    @Override
    public void
    forEach(Consumer<? super Leaf<Key, Value>> action) {
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<Leaf<Key, Value>>
    spliterator() {
        return formList().spliterator();
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
            if (previous == null || getKey(point).compareTo(previous) > 0) previous = getKey(point);
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
            int comparison = getKey(mid).compareTo(key);
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
