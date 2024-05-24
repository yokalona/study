package com.yokalona.array.lazy;

import com.yokalona.array.lazy.configuration.Configuration;
import com.yokalona.array.lazy.subscriber.Subscriber;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.yokalona.array.lazy.CompactInteger.compact;
import static com.yokalona.array.lazy.configuration.Chunked.chunked;
import static com.yokalona.array.lazy.configuration.Chunked.linear;
import static com.yokalona.array.lazy.configuration.File.Mode.*;
import static com.yokalona.array.lazy.configuration.File.file;
import static com.yokalona.array.lazy.configuration.Configuration.configure;
import static com.yokalona.tree.TestHelper.getSize;
import static com.yokalona.tree.TestHelper.shuffle;
import static org.junit.jupiter.api.Assertions.*;

class PersistentArrayTest {

    public static final int REPEATS = Power.two(5);
    public static final int MAX_CHUNK_SIZE = Power.two(19);
    private static final int[] size = {Power.two(5), Power.two(10)
            , Power.two(15), Power.two(20), Power.two(25), Power.two(30)};
    private static final float[] loadFactor = {.01F, .05F, .1F, .25F};

    private Path path;

    @BeforeEach
    public void
    setUp() throws IOException {
        this.path = Files.createTempDirectory("array");
    }

    @AfterEach
    public void
    tearDown() throws IOException {
        Files.list(path).map(Path::toFile).forEach(File::delete);
    }

    @Test
    public void testRecordWontPushoutOfChunkIfNotExceededQuota() {
        TestSubscriber subscriber = new TestSubscriber();
        Configuration configuration = configure(
                file(path.resolve("lrlwlm.la")).mode(RWD).cached())
                .memory(chunked(100))
                .addSubscriber(subscriber)
                .read(linear())
                .write(linear());
        try (var array = new PersistentArray<>(10, CompactInteger.descriptor, PersistentArray.FixedObjectLayout::new, configuration)) {
            for (int index = 0; index < 10; index++) {
                array.set(index, compact(index));
            }
        }
    }

    @Test
    public void testSameRecordWontPushoutOfChunkAsItCountAsOneOperation() {
        TestSubscriber subscriber = new TestSubscriber();
        Configuration configuration = configure(
                file(path.resolve("lrlwlm.la")).mode(RWD).cached())
                .memory(linear())
                .addSubscriber(subscriber)
                .read(linear())
                .write(linear());
        try (var array = new PersistentArray<>(10, CompactInteger.descriptor, PersistentArray.FixedObjectLayout::new, configuration)) {
            for (int index = 0; index < 10; index++) {
                array.set(0, compact(0));
            }
        }
    }

    @Test
    public void
    testLinearReadWriteMemory() {
        TestSubscriber subscriber = new TestSubscriber();
        Configuration configuration = configure(
                file(path.resolve("lrlwlm.la")).mode(RWD).cached())
                .memory(linear())
                .addSubscriber(subscriber)
                .read(linear())
                .write(linear());
        try (var array = new PersistentArray<>(100, CompactInteger.descriptor, PersistentArray.FixedObjectLayout::new, configuration)) {
            for (int index = 0; index < 100; index++) {
                array.set(index, compact(index));
//                assertEquals(index, subscriber.load.getLast());
                assertEquals(index, subscriber.serialized.getLast());
//                if (index > 0) assertEquals(index - 1, subscriber.unload.getLast());
            }
        }
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            assertTrue(subscriber.deserialized.isEmpty());
            for (int index = 0; index < 100; index++) {
                assertEquals(index, array.get(index).value());
//                assertEquals(index, subscriber.load.getLast());
                assertEquals(index, subscriber.deserialized.getLast());
//                if (index > 0) assertEquals(index - 1, subscriber.unload.getLast());
            }
        }
    }

    @Test
    public void
    testChunkedReadLinearWriteMemory() {
        TestSubscriber subscriber = new TestSubscriber();
        Configuration configuration = configure(
                file(path.resolve("crlwlm.la")).mode(RWD).cached())
                .memory(linear())
                .addSubscriber(subscriber)
                .read(chunked(10))
                .write(linear());
        try (var array = new PersistentArray<>(100, CompactInteger.descriptor, PersistentArray.FixedObjectLayout::new, configuration)) {
            for (int index = 0; index < 100; index++) {
                array.set(index, compact(index));
//                assertEquals(index, subscriber.load.getLast());
                assertEquals(index, subscriber.serialized.getLast());
//                if (index > 10) assertEquals(index - 10, subscriber.unload.getLast());
            }
        }
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            assertTrue(subscriber.deserialized.isEmpty());
            for (int index = 0; index < 100; index++) {
                assertEquals(index, array.get(index).value());
                if (index % 10 == 0) {
                    for (int i = index; i < Math.min(100, index + 10); i++) {
                        assertEquals(i, subscriber.deserialized.get(i - index));
                    }
                    subscriber.deserialized.clear();
                } else assertTrue(subscriber.deserialized.isEmpty());
            }
        }
    }

    @Test
    public void
    testChunkedReadWriteLinearMemory() {
        TestSubscriber subscriber = new TestSubscriber();
        Configuration configuration = configure(
                file(path.resolve("crlwlm.la")).mode(RWD).cached())
                .memory(chunked(10))
                .addSubscriber(subscriber)
                .read(chunked(10))
                .write(chunked(10));
        try (var array = new PersistentArray<>(100, CompactInteger.descriptor, PersistentArray.FixedObjectLayout::new, configuration)) {
            for (int index = 0; index < 100; index++) {
                array.set(index, compact(index));
//                assertEquals(index, subscriber.load.getLast());
                if ((index + 1) % 10 == 0) {
                    for (int i = 0; i < 10; i++) {
                        assertEquals(i + (index / 10) * 10, subscriber.serialized.get(i));
                    }
                    subscriber.serialized.clear();
                }
//                if (index > 10) assertEquals(index - 10, subscriber.unload.getLast());
            }
        }
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            assertTrue(subscriber.deserialized.isEmpty());
            for (int index = 0; index < 100; index++) {
                assertEquals(index, array.get(index).value());
                if (index % 10 == 0) {
                    for (int i = index; i < Math.min(100, index + 10); i++) {
                        assertEquals(i, subscriber.deserialized.get(i - index));
                    }
                    subscriber.deserialized.clear();
                } else assertTrue(subscriber.deserialized.isEmpty());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("generate")
    public void
    testRabbitAndTheHat(PersistenceTest test) throws IOException {
        Statist statist = new Statist();
        int length = test.size;
        Configuration configuration = configure(
                file(path.resolve("rabbitAndTheHat.la"))
                        .mode(RW)
                        .buffer(5 * Power.two(10))
                        .cached())
                .memory(chunked(test.memory))
                .addSubscriber(statist)
                .read(chunked(test.read))
                .write(chunked(test.write));
        long writeLinear = 0L, readLinear = 0L, writeRandom = 0L, readRandom = 0L;
        printConfiguration(new PersistenceTest(length, configuration.memory().size(),
                configuration.read().size(),
                configuration.write().size()));
        for (int i = 0; i < REPEATS; i++) {
            printHeader(i + 1);
            printStatistic();
            writeLinear += printStatistic("Linear write", writeLinear(length, configuration), statist);
            readLinear += printStatistic("Linear read", readLinear(length, configuration, true), statist);
            writeRandom += printStatistic("Random write", writeRandom(length, configuration), statist);
            readRandom += printStatistic("Random read", readRandom(length, configuration, true), statist);
        }
        printHeader();
        printAverage("linear write", (float) writeLinear / REPEATS, "ms");
        printAverage("linear read", (float) readLinear / REPEATS, "ms");
        printAverage("random write", (float) writeRandom / REPEATS, "ms");
        printAverage("random read", (float) readRandom / REPEATS, "ms");
        printAverage("collisions", (float) statist.fullCollisions / REPEATS, "ops");
        printHeader();
    }

    @Test
    public void
    testMerge() {
        TestSubscriber subscriber = new TestSubscriber();
        Configuration configuration1 = configure(
                file(path.resolve("merge1.la")).mode(RWD).cached())
                .memory(chunked(10))
                .addSubscriber(subscriber)
                .read(chunked(10))
                .write(chunked(10));
        Configuration configuration2 = configure(
                file(path.resolve("merge2.la")).mode(RWD).cached())
                .memory(chunked(10))
                .addSubscriber(subscriber)
                .read(chunked(10))
                .write(chunked(10));
        writeLinear(1000, configuration1);
        try (var array1 = PersistentArray.deserialize(CompactInteger.descriptor, configuration1);
             var array2 = new PersistentArray<>(1000, CompactInteger.descriptor, PersistentArray.FixedObjectLayout::new, configuration2)) {
            for (int index = 0; index < array2.length(); index++) assertNull(array2.get(index));
            PersistentArray.copy(array1, 0, array2, 0, 1000);
            for (int index = 0; index < array2.length(); index++) assertEquals(index, array2.get(index).value());
        }
    }

    @ParameterizedTest
    @ValueSource(floats = {.05F})
    public void
    testMemoryConsumption(float factor) throws IOException, InterruptedException {
        Statist statist = new Statist();
        int length = Power.two(30);
        Configuration configuration = configure(
                file(path.resolve("rabbitAndTheHat.la"))
                        .mode(RW)
                        .buffer(5 * Power.two(10))
                        .cached())
                .memory(chunked(Power.two(15)))
                .addSubscriber(statist)
                .read(chunked(Power.two(15)))
                .write(chunked(Power.two(15)));
        long writeLinear = 0L, readLinear = 0L, writeRandom = 0L, readRandom = 0L;
        printConfiguration(new PersistenceTest(length, configuration.memory().size(),
                configuration.read().size(),
                configuration.write().size()));
        for (int i = 0; i < 1; i++) {
            printHeader(i + 1);
            printStatistic();
            writeLinear += printStatistic("Linear write", writeLinear(length, configuration), statist);
            readLinear += printStatistic("Linear read", readLinear(length, configuration, false), statist);
//            writeRandom += printStatistic("Random write", writeRandom(length, configuration), statist);
//            readRandom += printStatistic("Random read", readRandom(length, configuration, false), statist);
        }
        printHeader();
        printAverage("linear write", (float) writeLinear / REPEATS, "ms");
        printAverage("linear read", (float) readLinear / REPEATS, "ms");
        printAverage("random write", (float) writeRandom / REPEATS, "ms");
        printAverage("random read", (float) readRandom / REPEATS, "ms");
        printAverage("collisions", (float) statist.fullCollisions / REPEATS, "ops");
        printHeader();
    }

    private static long
    readLinear(int length, Configuration configuration, boolean validate) {
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            long start = System.currentTimeMillis();
            array.get(10);
            for (int index = 0; index < length; index++) {
                CompactInteger value = array.get(index);
                if (validate) {
                    assertNotNull(value);
                    assertEquals(index, value.value());
                }
                printProgress(length, index);
            }
            System.err.print("\r");
            System.err.flush();
            return System.currentTimeMillis() - start;
        }
    }

    private static long
    readRandom(int length, Configuration configuration, boolean validate) {
        int[] indices = new int[length];
        for (int index = 0; index < length; index++) {
            indices[index] = index;
        }
        shuffle(indices);
//        indices = new int[] {624, 941, 609, 742, 719, 312, 580, 439, 633, 908, 90, 750, 263, 961, 856, 79, 786, 1004, 784, 991, 217, 958, 913, 286, 684, 744, 137, 266, 270, 740, 69, 164, 906, 20, 268, 191, 329, 10, 468, 157, 889, 73, 985, 600, 494, 44, 476, 595, 1018, 901, 46, 497, 85, 790, 977, 345, 999, 340, 658, 327, 826, 163, 630, 903, 1, 149, 847, 298, 998, 659, 359, 123, 33, 540, 956, 970, 328, 1000, 493, 805, 942, 714, 380, 651, 19, 986, 1023, 50, 315, 618, 363, 637, 209, 353, 365, 616, 927, 652, 402, 391, 134, 799, 41, 406, 7, 262, 909, 401, 103, 979, 560, 762, 753, 854, 83, 75, 810, 333, 377, 280, 47, 386, 311, 314, 352, 470, 234, 446, 1014, 662, 667, 978, 139, 478, 575, 727, 582, 524, 279, 119, 24, 694, 774, 460, 131, 407, 806, 72, 411, 76, 506, 92, 133, 84, 463, 332, 503, 426, 698, 30, 455, 623, 53, 894, 646, 348, 793, 900, 150, 853, 243, 596, 556, 644, 569, 587, 602, 273, 798, 224, 4, 804, 466, 773, 513, 181, 896, 58, 322, 613, 796, 868, 628, 676, 578, 2, 661, 837, 910, 330, 682, 236, 23, 764, 107, 936, 820, 166, 135, 124, 128, 562, 825, 282, 1017, 245, 708, 360, 1020, 57, 590, 290, 945, 419, 354, 381, 567, 891, 593, 256, 755, 834, 469, 142, 918, 417, 31, 480, 859, 43, 440, 68, 80, 846, 95, 922, 127, 792, 136, 274, 781, 973, 152, 898, 218, 959, 308, 144, 32, 201, 988, 673, 171, 499, 151, 87, 870, 915, 146, 591, 355, 251, 475, 610, 665, 473, 368, 457, 931, 51, 489, 205, 756, 16, 968, 592, 664, 974, 992, 850, 710, 204, 760, 948, 316, 431, 451, 712, 208, 109, 801, 412, 747, 544, 215, 606, 490, 984, 549, 877, 393, 840, 821, 172, 730, 695, 538, 617, 239, 746, 264, 671, 925, 514, 321, 344, 690, 229, 223, 767, 303, 423, 465, 253, 335, 35, 183, 721, 373, 586, 531, 427, 967, 42, 430, 230, 635, 872, 448, 534, 222, 495, 881, 487, 546, 372, 775, 168, 22, 479, 496, 787, 768, 324, 533, 526, 438, 188, 771, 1013, 794, 819, 907, 52, 844, 952, 895, 17, 770, 525, 207, 765, 210, 758, 947, 307, 21, 436, 404, 831, 733, 285, 112, 275, 387, 361, 405, 369, 648, 292, 867, 259, 252, 158, 718, 686, 502, 836, 751, 86, 216, 735, 522, 880, 858, 319, 297, 169, 397, 707, 914, 550, 724, 82, 548, 343, 221, 306, 660, 517, 869, 325, 803, 702, 537, 358, 200, 882, 519, 254, 769, 982, 366, 160, 692, 689, 126, 293, 113, 699, 860, 696, 211, 636, 766, 265, 98, 892, 326, 93, 467, 409, 179, 573, 186, 167, 197, 302, 277, 194, 535, 331, 148, 182, 857, 717, 912, 981, 281, 874, 539, 1021, 447, 851, 884, 558, 824, 902, 926, 838, 811, 371, 663, 631, 212, 782, 878, 111, 754, 421, 202, 726, 1019, 795, 543, 261, 250, 817, 510, 173, 954, 74, 949, 55, 500, 779, 462, 117, 271, 485, 966, 672, 376, 937, 675, 153, 627, 515, 477, 70, 816, 449, 185, 608, 445, 597, 138, 705, 728, 300, 214, 347, 647, 193, 650, 11, 713, 257, 165, 384, 88, 547, 738, 653, 390, 283, 620, 320, 66, 599, 585, 815, 741, 989, 299, 576, 81, 34, 865, 1005, 488, 18, 842, 461, 225, 349, 897, 971, 706, 841, 551, 574, 464, 845, 508, 471, 693, 716, 1006, 1002, 337, 437, 944, 722, 917, 809, 521, 29, 963, 338, 876, 501, 829, 518, 832, 611, 61, 45, 943, 474, 807, 3, 621, 685, 813, 789, 309, 334, 40, 99, 206, 78, 287, 933, 855, 557, 89, 552, 1007, 258, 351, 555, 649, 362, 607, 883, 379, 757, 669, 180, 106, 141, 272, 565, 899, 996, 255, 1003, 823, 645, 418, 997, 192, 428, 545, 828, 797, 812, 529, 833, 752, 732, 130, 121, 928, 38, 1022, 622, 175, 27, 260, 679, 632, 77, 864, 542, 233, 147, 994, 964, 392, 454, 356, 456, 284, 802, 425, 638, 866, 603, 619, 688, 655, 571, 483, 666, 505, 26, 122, 432, 305, 491, 615, 37, 938, 674, 12, 670, 143, 736, 849, 919, 888, 772, 680, 929, 49, 203, 955, 248, 435, 100, 190, 104, 323, 612, 374, 492, 125, 0, 923, 711, 383, 289, 458, 154, 364, 60, 6, 269, 875, 395, 626, 174, 226, 410, 101, 639, 975, 604, 704, 170, 398, 921, 71, 960, 291, 14, 743, 568, 785, 434, 56, 687, 246, 629, 145, 839, 528, 304, 852, 939, 737, 267, 780, 1008, 472, 132, 9, 962, 863, 691, 861, 64, 583, 54, 403, 572, 934, 700, 843, 559, 443, 120, 553, 683, 400, 1012, 697, 725, 13, 244, 594, 313, 28, 318, 161, 184, 950, 887, 830, 413, 336, 761, 641, 414, 862, 288, 342, 105, 1010, 729, 579, 1009, 776, 678, 198, 783, 278, 199, 228, 196, 890, 778, 459, 818, 108, 723, 442, 422, 946, 140, 570, 614, 577, 25, 357, 481, 791, 159, 484, 388, 429, 957, 563, 800, 238, 408, 498, 220, 581, 1016, 932, 532, 643, 504, 523, 759, 15, 242, 115, 389, 916, 375, 433, 416, 232, 709, 350, 920, 827, 969, 822, 990, 642, 634, 788, 97, 177, 195, 116, 420, 983, 247, 219, 450, 904, 385, 96, 187, 885, 346, 605, 924, 156, 370, 339, 424, 953, 598, 879, 972, 512, 5, 237, 588, 893, 48, 444, 102, 905, 8, 235, 1001, 993, 453, 39, 213, 301, 452, 67, 189, 91, 227, 94, 808, 935, 231, 294, 701, 748, 1011, 677, 110, 415, 62, 886, 520, 681, 731, 114, 668, 835, 527, 871, 965, 507, 296, 848, 734, 980, 777, 749, 564, 745, 589, 65, 276, 486, 1015, 930, 394, 176, 640, 763, 873, 178, 995, 63, 295, 739, 601, 59, 162, 656, 441, 554, 715, 310, 951, 987, 703, 155, 940, 482, 240, 541, 530, 511, 657, 976, 654, 118, 561, 720, 396, 249, 341, 382, 536, 814, 129, 625, 911, 399, 241, 516, 584, 317, 36, 367, 378, 509, 566};
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            long start = System.currentTimeMillis();
            int ops = 0;
            for (int index : indices) {
                CompactInteger value = array.get(index);
                if (validate) {
                    assertNotNull(value);
                    assertEquals(index, value.value());
                }
                printProgress(length, ++ops);
            }
            System.err.print("\r");
            System.err.flush();
            return System.currentTimeMillis() - start;
        }
    }

    private static long
    writeLinear(int length, Configuration configuration) {
        try (var array = new PersistentArray<>(length, CompactInteger.descriptor, PersistentArray.FixedObjectLayout::new, configuration)) {
            long start = System.currentTimeMillis();
            for (int index = 0; index < length; index++) {
                array.set(index, new CompactInteger(index));
                printProgress(length, index);
            }
            System.err.print("\r");
            System.err.flush();
            return System.currentTimeMillis() - start;
        }
    }

    private static void printProgress(int length, int index) {
        if (index % (length / 1000) == 0) {
            System.err.printf("\r|-record %10d out of %10d-----+--------------------|", index, length);
            System.err.flush();
        }
    }

    private static long
    writeRandom(int length, Configuration configuration) {
        int[] indices = new int[length];
        for (int index = 0; index < length; index++) {
            indices[index] = index;
        }
        shuffle(indices);
//        System.out.println(Arrays.toString(indices));
//        indices = new int[] {11, 5, 6, 13, 25, 21, 2, 12, 7, 28, 23, 10, 20, 22, 9, 8, 4, 14, 18, 24, 15, 29, 30, 27, 31, 26, 0, 16, 3, 17, 19, 1};
        try (var array = new PersistentArray<>(length, CompactInteger.descriptor, PersistentArray.FixedObjectLayout::new, configuration)) {
            long start = System.currentTimeMillis();
            int ops = 0;
            for (int index : indices) {
                array.set(index, new CompactInteger(index));
                printProgress(length, ++ops);
            }
            System.err.print("\r");
            System.err.flush();
            return System.currentTimeMillis() - start;
        }
    }

    public static void
    printConfiguration(PersistenceTest test) {
        System.out.printf("""
                        |=========================================+====================|
                        | Randomized Access Test(RAT)             |                    |
                        |=========================================+====================|
                        | Array size                              | %10d records |
                        |     Chunks                              |                    |
                        |         Read                            | %10d records |
                        |         Write                           | %10d records |
                        |         Memory                          | %10d records |%n""",
                test.size, test.read, test.write, test.memory);
    }

    public static void
    printHeader(int repeat) {
        String header = """
                |-----------------------------------------+--------------------|
                | Iteration                               |    %5d/%-5d     |%n""";
        System.out.printf(header, repeat, REPEATS);
    }

    public static void
    printStatistic() {
        System.out.println("""
                |-----------------------------------------+--------------------|
                |                                                              |""");
    }

    public static long
    printStatistic(String name, long time, Statist statist) {
        System.out.printf("""
                        |                                         |--------------------|
                        |     %-15s                     | %10d ms      |
                        |     Collisions                          | %10d ops     |
                        |     Serializations                      | %10d ops     |
                        |     Deserializations                    | %10d ops     |%n""",
                name, time, statist.collisions, statist.serialized, statist.deserialized);
        statist.reset();
        return time;
    }

    public static void
    printHeader() {
        System.out.println("|=========================================+====================|");
    }

    public static void
    printAverage(String name, float value, String measurement) {
        System.out.printf("""
                        |     Average %-15s             | %10.2f %-5s   |%n""",
                name, value, measurement);
    }

    record PersistenceTest(int size, int memory, int read, int write) {

        PersistenceTest {
            size = Math.max(1, size);
            memory = Math.max(1, memory);
            read = Math.max(1, read);
            write = Math.max(1, write);
        }
    }

    public static List<PersistenceTest>
    generate() {
        List<PersistenceTest> generated = new ArrayList<>();
        for (int size : size)
            for (float factor : loadFactor) {
                int chunkSize = Math.min(MAX_CHUNK_SIZE, (int) (size * factor));
                generated.add(new PersistenceTest(size, chunkSize, 1, chunkSize));
            }
        return generated;
    }

    static class TestSubscriber implements Subscriber {
        List<Integer> serialized = new ArrayList<>();
        List<Integer> deserialized = new ArrayList<>();

        @Override
        public void onSerialized(int index) {
            serialized.add(index);
        }

        @Override
        public void onDeserialized(int index) {
            deserialized.add(index);
        }
    }

    static class Statist implements Subscriber {
        public int serialized = 0, deserialized = 0, collisions = 0, fullCollisions = 0;

        @Override
        public void
        onSerialized(int index) {
            serialized++;
        }

        @Override
        public void
        onDeserialized(int index) {
            deserialized++;
        }

        @Override
        public void
        onCollision(int current, int next) {
            collisions++;
            fullCollisions++;
        }

        public void reset() {
            serialized = 0;
            collisions = 0;
            deserialized = 0;
        }
    }
}

/*

|--------------------------------------------------------------|
|     Average linear write                |       2.13 ms      |
|     Average linear read                 |       0.69 ms      |
|     Average random write                |       5.06 ms      |
|     Average random read                 |       8.13 ms      |
|     Average collisions                  |     116.88 ops     |
|--------------------------------------------------------------|
|     Average linear write                |       0.38 ms      |
|     Average linear read                 |       0.44 ms      |
|     Average random write                |       4.66 ms      |
|     Average random read                 |       6.56 ms      |
|     Average collisions                  |      78.84 ops     |
|--------------------------------------------------------------|
|     Average linear write                |       1.19 ms      |
|     Average linear read                 |       0.50 ms      |
|     Average random write                |       4.50 ms      |
|     Average random read                 |       9.72 ms      |
|     Average collisions                  |      43.66 ops     |
|--------------------------------------------------------------|
 */