package com.yokalona.tree;

import com.yokalona.tree.b.BTree;

public interface Tree<Key extends Comparable<Key>, Value> {
    Value
    get(Key key);

    boolean
    insert(Key key, Value value);

    boolean
    contains(Key key);

    boolean
    remove(Key key);

    int
    height();

    int
    size();

    void
    clear();

}
