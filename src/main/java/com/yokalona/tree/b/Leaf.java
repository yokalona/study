package com.yokalona.tree.b;

public record Leaf<Key extends Comparable<Key>, Value>(Key key, Value value, Node<Key, Value> link) {

    public static <Key extends Comparable<Key>, Value> Leaf<Key, Value>
    internal(Key key, Node<Key, Value> next) {
        return new Leaf<>(key, null, next);
    }

    public static <Key extends Comparable<Key>, Value> Leaf<Key, Value>
    external(Key key, Value value) {
        return new Leaf<>(key, value, null);
    }
}
