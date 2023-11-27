package com.yokalona.array;

import java.util.Arrays;

public class CircularArray<Value> {
    private final Value[] values;

    private CircularArray(Value[] values) {
        this.values = values;
    }

    public Value get(int idx) {
        return this.values[circular(idx)];
    }

    public void set(int idx, Value value) {
        this.values[circular(idx)] = value;
    }

    public int size() {
        return this.values.length;
    }

    public void rotate(int from, int to) {
        if (from > to) {
            shiftLeft(from);
            to += values.length - from;
            from = 0;
        }

        for (int idx = 0; idx <= (to - from) / 2; idx++)
            swap(values, from + idx, to - idx);
    }

    private int circular(int idx) {
        if (idx >= this.values.length) return circular(idx - this.values.length);
        else if (idx < 0) return circular(idx + this.values.length);
        else return idx;
    }

    private void shiftLeft(int delta) {
        Value[] copy = Arrays.copyOf(values, values.length);
        System.arraycopy(copy, delta, values, 0, values.length - delta);
        System.arraycopy(copy, 0, values, values.length - delta, delta);
    }

    private void shiftRight(int delta) {
        Value[] copy = Arrays.copyOf(values, values.length);
        System.arraycopy(copy, 0, values, delta, values.length - delta);
        System.arraycopy(copy, values.length - delta, values, 0, delta);
    }

    private void reverse() {
        for (int left = 0, right = values.length - 1; left < right; left++, right--) {
            swap(values, left, right);
        }
    }

    private void shuffle() {
        for (int idx = this.values.length - 1; idx > 0; idx--) {
            int element = (int) Math.floor(Math.random() * (idx + 1));
            swap(this.values, idx, element);
        }
    }

    public String toString() {
        return Arrays.toString(this.values);
    }

    private static <Value> void swap(Value[] arr, int left, int right) {
        Value tmp = arr[left];
        arr[left] = arr[right];
        arr[right] = tmp;
    }


    public int next(int idx) {
        return idx + 1 >= values.length ? 0 : idx + 1;
    }

}
