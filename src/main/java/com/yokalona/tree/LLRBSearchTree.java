package com.yokalona.tree;

import static com.yokalona.Validations.*;

public class LLRBSearchTree<Key extends Comparable<Key>, Value> {

    private Node<Key, Value> root;

    public void
    insert(Key key, Value value) {
        validateKey(key);
        if (root == null) root = new Node<>(key, value, Node.BLACK);
        else root = Node.insert(root, key, value);
        root.color = Node.BLACK;
    }

    public Value
    get(Key key) {
        validateKey(key);
        return Node.get(root, key);
    }

    public Key
    max() {
        if (root == null) return null;
        Node<Key, ?> right = root;
        while (right.right != null) {
            right = right.right;
        }
        return Node.key(right);
    }

    public Key
    min() {
        if (root == null) return null;
        Node<Key, ?> left = root;
        while (left.left != null) {
            left = left.left;
        }
        return Node.key(left);
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

    public void
    balance() {
        if (root != null) Node.balance(root);
    }

    private static class Node<Key extends Comparable<Key>, Value> {
        private static final boolean RED = true;
        private static final boolean BLACK = false;

        private final Key key;

        private int size;
        private Value value;
        private boolean color;
        private Node<Key, Value> left;
        private Node<Key, Value> right;

        public Node(Key key, Value value, boolean color) {
            assert key != null;

            this.key = key;
            this.value = value;
            this.color = color;
            this.size = 1;
        }

        public Value
        value() {
            return value;
        }

        private static <Key extends Comparable<Key>, Value> Node<Key, Value>
        insert(Node<Key, Value> node, Key key, Value value) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            if (node == null) return new Node<>(key, value, RED);

            final int comparison = key.compareTo(node.key);
            if (comparison < 0) node.left = insert(node.left, key, value);
            else if (comparison > 0) node.right = insert(node.right, key, value);
            else node.value = value;

            return balance(node);
        }

        private static <Key extends Comparable<Key>, Value> Node<Key, Value>
        balance(Node<Key, Value> root) {
            assert root != null : ROOT_SHOULD_HAVE_NON_NULL_VALUE;

            if (red(root.right) && !red(root.left)) root = rotateLeft(root);
            if (red(root.left) && red(root.left.left)) root = rotateRight(root);
            if (red(root.right) && red(root.left)) flip(root);

            // update ranking
            root.size = 1 + size(root.left) + size(root.right);

            return root;
        }

        private static <Key extends Comparable<Key>, Value> Node<Key, Value>
        rotateLeft(Node<Key, Value> root) {
            assert root != null : ROOT_SHOULD_HAVE_NON_NULL_VALUE;
            assert red(root.right) && !red(root.left) : INCORRECT_COLORING;

            final Node<Key, Value> right = root.right;
            root.right = right.left;
            right.left = root;
            right.color = root.color;
            root.color = RED;
            right.size = root.size;
            root.size = 1 + size(root.left) + size(root.right);
            return right;
        }

        private static <Key extends Comparable<Key>, Value> Node<Key, Value>
        rotateRight(Node<Key, Value> root) {
            assert root != null : ROOT_SHOULD_HAVE_NON_NULL_VALUE;
            assert red(root.left) && red(root.left.left) : INCORRECT_COLORING;

            final Node<Key, Value> left = root.left;
            root.left = left.right;
            left.right = root;
            left.color = root.color;
            root.color = RED;
            left.size = root.size;
            root.size = 1 + size(root.left) + size(root.right);
            return left;
        }

        private static void
        flip(Node<?, ?> root) {
            assert root != null : ROOT_SHOULD_HAVE_NON_NULL_VALUE;
            assert red(root.left) && red(root.right) : INCORRECT_COLORING;

            root.color = !root.color;
            root.left.color = !root.left.color;
            root.right.color = !root.right.color;
        }

        private static <Key extends Comparable<Key>, Value> Value
        get(Node<Key, Value> root, Key key) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            while (root != null) {
                final int comparison = root.key.compareTo(key);
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

        private static boolean
        red(Node<?, ?> node) {
            if (node == null) return BLACK;
            return node.color;
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
