package com.yokalona.tree;

public class BinarySearchTree<Key extends Comparable<Key>, Value> {

    private Node<Key, Value> root;

    public void
    insert(Key key, Value value) {
        root = Node.put(root, key, value);
    }

    public Value
    get(Key key) {
        return Node.value(root);
    }

    public int
    size() {
        if (root == null) return 0;
        return root.size;
    }

    public int
    height() {
        if (root == null) return 0;
        return Node.height(root);
    }

    private static class Node<Key extends Comparable<Key>, Value> {
        private final Key key;

        private int size;
        private Value value;
        private Node<Key, Value> left;
        private Node<Key, Value> right;

        public Node(Key key, Value value) {
            assert key != null;

            this.key = key;
            this.value = value;
            this.size = 1;
        }

        public Value
        value() {
            return value;
        }

        private static <Key extends Comparable<Key>, Value> Node<Key, Value>
        put(Node<Key, Value> node, Key key, Value value) {
            assert key != null;

            if (node == null) return new Node<>(key, value);

            final int comparison = node.key.compareTo(key);
            if (comparison < 0) node.right = put(node.right, key, value);
            else if (comparison > 0) node.left = put(node.left, key, value);
            else node.value = value;

            // update ranking
            node.size = 1 + size(node.left) + size(node.right);

            return node;
        }

        private static <Key extends Comparable<Key>, Value> Value
        get(Node<Key, Value> root, Key key) {
            assert key != null;

            while (root != null) {
                int comparison = root.key.compareTo(key);
                if (comparison > 0) root = root.left;
                else if (comparison < 0) root = root.right;
                else return root.value;
            }
            return null;
        }

        private static <Value> Value
        value(Node<?, Value> root) {
            if (root == null) return null;
            return root.value;
        }

        private static <Key extends Comparable<Key>> Key
        key(Node<Key, ?> root) {
            if (root == null) return null;
            return root.key;
        }

        private static int
        size(Node<?, ?> node) {
            if (node == null) return 0;
            return node.size;
        }

        private static int
        height(Node<?, ?> node) {
            if (node == null) return -1;
            return 1 + Math.max(height(node.left), height(node.right));
        }

    }
}
