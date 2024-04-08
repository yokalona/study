package com.yokalona.tree.b;

import java.util.Random;

public class Helper {

    public static final Random RANDOM = new Random();

    public static Integer randomKey(Integer[] data) {
        return data[RANDOM.nextInt(data.length)];
    }

    public static Integer randomNumber() {
        return RANDOM.nextInt();
    }

    public static Integer[] formSample(int sampleSize) {
        Integer[] data = new Integer[sampleSize];
        for (int testSize = 0; testSize < sampleSize; testSize++)
            data[testSize] = testSize;
        shuffle(data);
        return data;
    }

    public static <Type extends Comparable<Type>> void shuffle(Type[] array) {
        for (int idx = array.length - 1; idx > 0; idx--) {
            int element = (int) Math.floor(Math.random() * (idx + 1));
            swap(array, idx, element);
        }
    }

    private static <Type extends Comparable<Type>> void swap(Type[] arr, int left, int right) {
        Type tmp = arr[left];
        arr[left] = arr[right];
        arr[right] = tmp;
    }
}
