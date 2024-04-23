package com.yokalona.tree.b;

import java.util.Arrays;

public class DataUnit<Key extends Comparable<Key>, Value> {

    Array<Key> keys;
    Array<Value> values;

    public void
    insert(Key key, Value value) {
        keys.insert(key);
        values.insert(value);
    }

    public static class Array<Type> {
        Type[] array;
        int size;

        public Array(int capacity, Class<Type> typeClass) {
            this.array = (Type[]) java.lang.reflect.Array.newInstance(typeClass, capacity);
        }


        public Type
        get(int index) {
            return array[index];
        }

        public void
        replace(int index, Type value) {
            array[index] = value;
        }

        public int
        insert(Type value) {
            array[size] = value;
            return size++;
        }

        public int
        insert(int index, Type value) {
            System.arraycopy(array, index, array, index + 1, size - index);
            replace(index, value);
            return size++;
        }

        public void
        remove(int index) {
            System.arraycopy(array, index + 1, array, index, size - index - 1);
            array[-- size] = null;
        }

        public void
        remove(int from, int to) {
            Arrays.fill(array, from, to, null);
            size -= to - from;
        }
    }
}
