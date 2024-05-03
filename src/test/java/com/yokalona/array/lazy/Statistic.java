package com.yokalona.array.lazy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import static com.yokalona.Validations.KEY_SHOULD_HAVE_NON_NULL_VALUE;

public class Statistic {

    public static final Random RANDOM = new Random();

    @ParameterizedTest
    @ValueSource(ints = {10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000})
    public void stat(int iterations) {
        Array array = new Array(10);
        for (int i = 0; i < array.data.length; i ++) {
            array.set(i, RANDOM.nextInt());
        }
        Arrays.sort(array.data);
        for (int i = 0; i < iterations; i ++) {
            array.equal(RANDOM.nextInt());
        }
        record Access(int index, int count) {}
        Access[] accesses = new Access[array.access.length];
        for (int i = 0; i < array.access.length; i ++) {
            accesses[i] = new Access(i, array.access[i]);
        }
        Arrays.sort(accesses, Comparator.comparingInt(a -> a.count));
        for (Access access : accesses) {
            System.out.println(access);
        }
    }

    static class Array {
        private final int[] data;
        private final int[] access;

        Array(int length) {
            this.data = new int[length];
            this.access = new int[length];
        }

        public void
        set(int index, int value) {
            this.access[index] ++;
            this.data[index] = value;
        }

        public int
        get(int index) {
            this.access[index] ++;
            return this.data[index];
        }

        public int
        equal(final int value) {
            int left = 0, right = data.length - 1;
            while (left <= right) {
                int mid = left + (right - left) / 2;
                int comparison = Integer.compare(get(mid), value);
                if (comparison > 0) right = mid - 1;
                else if (comparison < 0) left = mid + 1;
                else return mid;
            }
            return - (left + 1);
        }
    }
}
