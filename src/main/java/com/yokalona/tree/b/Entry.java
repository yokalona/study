package com.yokalona.tree.b;

public record Entry<Key extends Comparable<Key>, Value>
        (Key key, Value value) { }
