package com.yokalona.tree.b;

import java.util.HashMap;
import java.util.Map;

import static com.yokalona.Validations.*;

public class Node<Key extends Comparable<Key>, Value> {
    private final boolean leaf;
    private final DataBlock<Key, Value> children;

    Node(final int capacity, boolean leaf) {
        assert capacity > 2 : CAPACITY_SHOULD_BE_GREATER_THAN_2;
        assert capacity % 2 == 0 : CAPACITY_SHOULD_BE_EVEN;

        this.leaf = leaf;
        this.children = new DataBlock<>(capacity, leaf);
    }

    boolean
    leaf() {
        return leaf;
    }

    public DataBlock<Key, Value>
    children() {
        return children;
    }

    private Node<Key, Value>
    split(boolean leaf) {
        final Node<Key, Value> split = new Node<>(children.length(), leaf);
        children.splitWith(split.children);
        return split;
    }

    private void
    balance(final Node<Key, Value> parent, final int index) {
        final Node<Key, Value> left = leftSibling(parent, index);
        final Node<Key, Value> right = rightSibling(parent, index);
        if (right != null && right.haveSpare()) rotateLeft(parent, index, right);
        else if (left != null && left.haveSpare()) rotateRight(parent, index, left);
        else if (left != null) mergeWith(left, parent, index);
        else if (right != null) right.mergeWith(this, parent, index + 1);
    }

    private boolean
    haveSpare() {
        return children.size() > children.length() / 2;
    }

    private boolean
    deficient() {
        return children.size() < children.length() / 2;
    }

    private void
    mergeWith(Node<Key, Value> other, Node<Key, Value> parent, int index) {
        children.mergeWith(other.children);
        parent.children.remove(index);
    }

    public Map<Key, Value>
    extract() {
        Map<Key, Value> map = new HashMap<>();
        for (int child = 0; child < children.size(); child++) {
            map.put(children.key(child), children.value(child));
        }
        return map;
    }

    public Value
    getBy(final Key key, int height) {
        DataBlock<Key, Value> dataBlock = getDatablock(key, height);
        final int child = dataBlock.equal(key);
        if (child < 0 || child >= dataBlock.size()) return null;
        return dataBlock.value(child);
    }

    public boolean
    contains(final Key key, int height) {
        DataBlock<Key, Value> dataBlock = getDatablock(key, height);
        final int child = dataBlock.equal(key);
        if (child < 0 || child >= dataBlock.size()) return false;
        return dataBlock.key(child) != null;
    }

    private DataBlock<Key, Value>
    getDatablock(final Key key, int height) {
        assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
        assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

        Node<Key, Value> next = this;
        while (height > 0) {
            int node = next.children.greaterThan(key) - 1;
            next = next.children.link(node);
            height--;
        }
        return next.children;
    }

    public Key
    getRank(int rank) {
        assert rank >= 0 && rank < children.size() : EXCEEDING_CAPACITY;

        return children.key(rank);
    }

    public Result<Key, Value>
    insertBy(final Key key, final Value value, int height) {
        assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
        assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

        Result<Key, Value> result;
        if (height == 0) result = insertLeaf(key, value);
        else result = insertNode(key, value, height);
        if (result.inserted && children.size() >= children.length()) return result.node(split(height == 0));
        else return result.node(null);
    }

    private Result<Key, Value>
    insertLeaf(final Key key, final Value value) {
        assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

        int child = children.equal(key);
        if (child < 0) {
            children.insert(- child - 1, key, value);
            return new Result<>(true);
        } else {
            children.replace(child, key, value);
            return new Result<>(false);
        }
    }

    private Result<Key, Value>
    insertNode(final Key key, final Value value, final int height) {
        assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
        assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

        int node = children.greaterThan(key) - 1;
        if (node < 0) node = 0;
        final Result<Key, Value> result = children.link(node).insertBy(key, value, height - 1);
        if (result.inserted && result.node != null) {
            children.replace(node, children.link(node).children().minKey(), children.link(node));
            children.insert(node + 1, result.node.children().minKey(), result.node);
        }
        return result.node(null);
    }


    public static class Result<Key extends Comparable<Key>, Value> {
        private final boolean inserted;
        private Node<Key, Value> node;

        private Result(boolean inserted) {
            this.inserted = inserted;
        }

        public boolean
        inserted() {
            return inserted;
        }

        public Node<Key, Value>
        node() {
            return node;
        }

        public Result<Key, Value>
        node(Node<Key, Value> node) {
            this.node = node;
            return this;
        }
    }

    public boolean
    removeBy(final Key key, final int height) {
        return removeBy(key, null, - 1, height);
    }

    private boolean
    removeBy(final Key key, final Node<Key, Value> parent, final int index, final int height) {
        assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
        assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

        if (height == 0) return removeLeaf(key, parent, index);
        else return removeNode(key, parent, index, height);
    }

    private boolean
    removeBy(final Key key) {
        assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

        final int child = children.equal(key);
        if (child < 0) return false;
        children.remove(child);
        return true;
    }

    private boolean
    removeNode(Key key, Node<Key, Value> parent, int index, int height) {
        int node = children.greaterThan(key) - 1;
        if (node >= children.size()) return false;
        final boolean removed = children.link(node).removeBy(key, this, node, height - 1);
        if (deficient()) balance(parent, index);
        return removed;
    }

    private boolean
    removeLeaf(Key key, Node<Key, Value> parent, int index) {
        final boolean removed = removeBy(key);
        if (deficient()) balance(parent, index);
        return removed;
    }

    private void
    removeMin() {
        children.remove(0);
    }

    private void
    removeMax() {
        children.remove(children.size() - 1);
    }

    private void
    rotateLeft(Node<Key, Value> parent, int index, Node<Key, Value> right) {
        children.insertMinFrom(right.children);
        parent.children.replace(index + 1, right.getRank(1), right);
        right.removeMin();
    }

    private void
    rotateRight(Node<Key, Value> parent, int index, Node<Key, Value> left) {
        children.insertMaxFrom(0, left.children);
        parent.children.replace(index, left.children.maxKey(), this);
        left.removeMax();
    }

    private Node<Key, Value>
    leftSibling(final Node<Key, Value> parent, final int index) {
        if (parent != null && index > 0) return parent.children.link(index - 1);
        return null;
    }

    private Node<Key, Value>
    rightSibling(final Node<Key, Value> parent, final int index) {
        if (parent != null && index < parent.children.size() - 1) return parent.children.link(index + 1);
        return null;
    }
}
