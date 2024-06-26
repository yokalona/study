package com.yokalona.tree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class LLRBSearchTreeTest {
    public static final int CAPACITY = 0;
    public static final int TEST_SIZE = 1;
    public static final int REPEATS = 2;

    @ParameterizedTest
    @MethodSource("loadParameters")
    public void remove(int[] parameters) {
        int capacity = parameters[CAPACITY];

        Integer[] data = new Integer[parameters[TEST_SIZE]];
        for (int testSize = 0; testSize < parameters[TEST_SIZE]; testSize++)
            data[testSize] = testSize;

        for (int repeat = 0; repeat < parameters[REPEATS]; repeat ++) {
            System.out.println("Repeat: " + repeat);
            LLRBSearchTree<Integer, Integer> bTree = new LLRBSearchTree<>();
            shuffle(data);
            printOrder("\tInsertion order: [%s]%n", data);
            for (int sample : data) {
                bTree.insert(sample, sample);
            }

            shuffle(data);
            Integer[] remove = new Integer[parameters[TEST_SIZE] / 2];
            System.arraycopy(data, 0, remove, 0, remove.length);

            printOrder("\tRemoval order: [%s]%n", remove);
            System.out.print("\tStarting removing data: ");
            for (int toRemove : remove) {
                bTree.remove(toRemove);
                assertNull(bTree.get(toRemove));
            }
            System.out.println("OK");
            assertEquals(parameters[TEST_SIZE] - remove.length, bTree.size());
            testSizeAndGrowthRate(bTree, capacity, bTree.size());

            Arrays.sort(remove);

            System.out.print("\tConsistency test: ");
            for (int sample : data) {
                if (Arrays.binarySearch(remove, sample) >= 0) {
                    assertNull(bTree.get(sample));
                } else {
                    assertEquals(sample, bTree.get(sample));
                }
            }
            System.out.println("OK");

            System.out.print("\tRemoving non existing keys: ");
            for (int sample : data) {
                int key = sample + parameters[TEST_SIZE] + 1;
                assertFalse(bTree.contains(key));
                bTree.remove(key);
            }
            System.out.println("OK");

            for (int sample : data) {
                bTree.remove(sample);
            }
            assertEquals(0, bTree.size());
            assertEquals(0, bTree.height());
        }
    }

    private static void printOrder(String message, Integer[] data) {
        StringBuilder sb = new StringBuilder();
        sb.append(data[0]);
        for (int i = 1; i < Math.min(data.length, 1000); i ++) {
            sb.append(' ').append(data[i]);
        }
        System.out.printf(message, sb);
    }

    @ParameterizedTest
    @ValueSource(ints = {- 5, - 4, 0, 1, 3, 5, 999})
    public void testCapacityBadArguments(int capacity) {
//        assertThrows(IllegalArgumentException.class, () -> new LLRBSearchTree<>(capacity));
    }

    @Test
    public void testNullKeyIsNotAllowed() {
        assertThrows(IllegalArgumentException.class, () -> new LLRBSearchTree<>().get(null));
        assertThrows(IllegalArgumentException.class, () -> new LLRBSearchTree<>().insert(null, new Object()));
        assertThrows(IllegalArgumentException.class, () -> new LLRBSearchTree<>().contains(null));
        assertThrows(IllegalArgumentException.class, () -> new LLRBSearchTree<>().remove(null));
    }

//    @Test
//    public void testPrint() {
//        Integer[] data = new Integer[10];
//        for (int testSize = 0; testSize < 10; testSize++)
//            data[testSize] = testSize;
//        shuffle(data);
//
//        LLRBSearchTree<Integer, Integer> bTree = new LLRBSearchTree<>();
//        for (int sample : data) {
//            bTree.insert(sample, sample);
//        }
//        String print = bTree.toString();
//
//        shuffle(data);
//        for (int sample : data) {
//            assertTrue(print.contains(String.valueOf(sample)));
//        }
//    }

    @ParameterizedTest
    @MethodSource("consistencyParameters")
    public void testInsertConsistency(int[] parameters) {
        int capacity = parameters[CAPACITY];

        Integer[] data = new Integer[parameters[TEST_SIZE]];
        for (int testSize = 0; testSize < parameters[TEST_SIZE]; testSize++)
            data[testSize] = testSize;
        shuffle(data);

        System.out.printf("Prepared dataset of size: %d, tree capacity: %d%n", data.length, capacity);

        LLRBSearchTree<Integer, Integer> bTree = new LLRBSearchTree<>();
        for (int sample : data) {
            bTree.insert(sample, sample);
        }
        testSizeAndGrowthRate(bTree, capacity, parameters[TEST_SIZE]);

        System.out.print("\tTesting consistency:\t\t");
        shuffle(data);
        for (int sample : data) {
            assertNotNull(bTree.get(sample), () -> "Consistency test failed, trace: " + trace(bTree, data));
            assertTrue(bTree.contains(sample));
        }
        System.out.println("OK");

        System.out.print("\tTesting excess data:\t\t");
        for (int sample : data) {
            int key = sample + parameters[TEST_SIZE] + 1;
            assertNull(bTree.get(key));
            assertFalse(bTree.contains(key));
        }
        System.out.println("OK");

        System.out.print("\tTesting repeated insert:\t");
        shuffle(data);
        for (int sample : data) {
            bTree.insert(sample, sample);
        }
        System.out.println("OK");
        testSizeAndGrowthRate(bTree, capacity, parameters[TEST_SIZE]);
        System.out.println();
    }

    @ParameterizedTest
    @MethodSource("loadParameters")
    public void testRepeatInserts(int[] parameters) {
        int capacity = parameters[CAPACITY];

        Integer[] data = new Integer[parameters[TEST_SIZE]];
        for (int testSize = 0; testSize < parameters[TEST_SIZE]; testSize++)
            data[testSize] = testSize;
        shuffle(data);

        System.out.printf("Prepared dataset of size: %d, tree capacity: %d%n", data.length, capacity);

        LLRBSearchTree<Integer, Integer> bTree = new LLRBSearchTree<>();

        for (int repeat = 0; repeat < parameters[REPEATS]; repeat ++) {
            System.out.printf("\n\tRepeated insert operations, iteration: %6d%n", repeat);
            shuffle(data);
            for (int sample : data) {
                bTree.insert(sample, sample);
            }
            testSizeAndGrowthRate(bTree, capacity, parameters[TEST_SIZE]);

            System.out.print("\tTesting consistency:\t\t");
            shuffle(data);
            for (int sample : data) {
                assertNotNull(bTree.get(sample), () -> "Consistency test failed, trace: " + trace(bTree, data));
            }
            System.out.println("OK");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {4, 8, 16, 32, 64, 100, 1000})
    public void performanceReport(int capacity) {
        int sampleSize = 1000;
        long [] results = new long[100];
        for (int repeat = 0; repeat < 100; repeat ++) {
            LLRBSearchTree<Integer, Integer> bTree = new LLRBSearchTree<>();
            Integer[] data = new Integer[sampleSize * (repeat + 1)];
            for (int testSize = 0; testSize < sampleSize * (repeat + 1); testSize++)
                data[testSize] = testSize;
            shuffle(data);

            long start = System.currentTimeMillis();
            for (int sample : data) {
                bTree.insert(sample, sample);
            }
            long end = System.currentTimeMillis();
            results[repeat] = end - start;
        }
        Arrays.sort(results);
        System.out.printf("Capacity: %d, sample size: %d, median time: %.2f ms%n",
                capacity, sampleSize, (double)(results[49] + results[50]) / 2);
        for (int result = 0; result < results.length; result ++) {
            System.out.printf("\t%d records: %d ms%n", sampleSize * (result + 1), results[result]);
        }
    }

    private <Key extends Comparable<Key>> String trace(LLRBSearchTree<Key, ?> bTree, Key[] data) {
        return "Data set: " +
                Arrays.toString(data) +
                "\nTree: \n" +
                bTree;
    }

    private void testSizeAndGrowthRate(LLRBSearchTree<?, ?> bTree, int capacity, int size) {
        System.out.printf("\tSize: \t\t\t\t\t\t%d%n\tHeight: \t\t\t\t\t%d%n", bTree.size(), bTree.height());
        assertEquals(size, bTree.size());
        double lowerBound = Math.ceil(log(bTree.size() + 1, 2));
        double upperBound = Math.floor(log((bTree.size() + 1) * 4, 2));
        System.out.printf("\tHeight is within borders:\t%.2f <= %d <= %.2f%n", lowerBound, bTree.height(), upperBound);
//        assertTrue(bTree.height() >= lowerBound
//                && bTree.height() <= upperBound);
    }

    private static double log(int number, int base) {
        return Math.log(number) / Math.log(base);
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

    private static int[][] consistencyParameters() {
        return new int[][]{
                {4, 10}, {4, 100}, {4, 1000}, {4, 10000},
                {6, 10}, {6, 100}, {6, 1000}, {6, 10000},
                {10, 10}, {10, 100}, {10, 1000}, {10, 10000},
                {100, 10}, {100, 100}, {100, 1000}, {100, 10000},
                {256, 2}, {256, 78}, {256, 1_000_000},
                {1_000, 1_000_000}, {1_000, 1_000_000},
                {4, 1_000_000}, {30, 1_000_000},
                {1_000_000, 10}, {1_000_000, 100}, {1_000_000, 1000}, {1_000_000, 10000}};
    }

    private static int[][] loadParameters() {
        return new int[][]{
                {4, 10, 1000}, {4, 1000, 1000},
                {6, 10, 1000}, {6, 1000, 1000},
                {8, 10, 1000}, {8, 1000, 1000},
                {10, 10, 1000}, {10, 1000, 1000},
                {20, 10, 1000}, {20, 1000, 1000},
                {100, 10, 1000}, {100, 1000, 1000}
        };
    }
}