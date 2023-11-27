package com.yokalona.array;

import java.util.BitSet;

public class BooleanArray {
    private final BitSet underlying;
    private final int size;

    public BooleanArray() {
        this(0);
    }

    public BooleanArray(int size) {
        this.size = size;
        this.underlying = new BitSet();
    }

    public BooleanArray increase() {
        BooleanArray newArray = new BooleanArray(size + 1);
        newArray.underlying.or(this.underlying);
        return newArray;
    }

    public int size() {
        return size;
    }

    public boolean is(int idx) {
        return underlying.get(idx);
    }

    public void set(int idx) {
        underlying.set(idx);
    }
}
