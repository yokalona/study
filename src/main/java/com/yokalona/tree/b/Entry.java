package com.yokalona.tree.b;

public record Entry<Key extends Comparable<Key>, Value>
        (Key key, Value value) {
    public static <Key extends Comparable<Key>, Value> Entry<Key, Value>
    fromLeaf(Leaf<Key, Value> leaf) {
        return new Entry<>(leaf.key(), leaf.value());
    }
}
