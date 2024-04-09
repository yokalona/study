package com.yokalona.tree.b;

import java.util.Random;

public class Helper {

    public static final Random RANDOM = new Random();

    public static SpyKey randomKey(SpyKey[] data) {
        return data[RANDOM.nextInt(data.length)];
    }

    public static Integer randomNumber() {
        return RANDOM.nextInt();
    }

    public static SpyKey[] formSample(int sampleSize) {
        SpyKey[] data = new SpyKey[sampleSize];
        for (int testSize = 0; testSize < sampleSize; testSize++)
            data[testSize] = new SpyKey(testSize);
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

    public record SpyKey(Integer key) implements Comparable<SpyKey> {
        private static long count = 0L;

        public static long count() {
            return count;
        }

        public static void reset() {
            count = 0L;
        }

        public int compareTo(SpyKey anotherInteger) {
            count++;
            return key.compareTo(anotherInteger.key);
        }
    }
}
