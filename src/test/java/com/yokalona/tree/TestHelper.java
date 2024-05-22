package com.yokalona.tree;

public class TestHelper {

    private static final long kilo = 1024;
    private static final long mega = kilo * kilo;
    private static final long giga = mega * kilo;
    private static final long tera = giga * kilo;

    public static double log(int number, int base) {
        return Math.log(number) / Math.log(base);
    }

    public static void
    shuffle(int[] array) {
        for (int idx = array.length - 1; idx > 0; idx--) {
            int element = (int) Math.floor(Math.random() * (idx + 1));
            swap(array, idx, element);
        }
    }

    public static <Type extends Comparable<Type>> void shuffle(Type[] array) {
        for (int idx = array.length - 1; idx > 0; idx--) {
            int element = (int) Math.floor(Math.random() * (idx + 1));
            swap(array, idx, element);
        }
    }

    private static void
    swap(int[] arr, int left, int right) {
        int tmp = arr[left];
        arr[left] = arr[right];
        arr[right] = tmp;
    }

    private static <Type extends Comparable<Type>> void swap(Type[] arr, int left, int right) {
        Type tmp = arr[left];
        arr[left] = arr[right];
        arr[right] = tmp;
    }

    public static String
    getSize(long size) {
        double kb = (double) size / kilo, mb = kb / kilo, gb = mb / kilo, tb = gb / kilo;
        if (size < kilo) return size + " b";
        else if (size < mega) return String.format("%.2f Kb", kb);
        else if (size < giga) return String.format("%.2f Mb", mb);
        else if (size < tera) return String.format("%.2f Gb", gb);
        else return String.format("%.2f Tb", tb);
    }
}
