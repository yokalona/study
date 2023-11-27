package com.yokalona.tree;

public class LLRBSearchTree<Key extends Comparable<Key>, Value> {

    private Node<Key, Value> root;

    public void
    insert(Key key, Value value) {
        if (root == null) root = new Node<>(key, value, Node.BLACK);
        else root = Node.put(root, key, value);
        root.color = Node.BLACK;
    }

    public Value
    get(Key key) {
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
        put(Node<Key, Value> node, Key key, Value value) {
            assert key != null;

            if (node == null) return new Node<>(key, value, RED);

            final int comparison = key.compareTo(node.key);
            if (comparison < 0) node.left = put(node.left, key, value);
            else if (comparison > 0) node.right = put(node.right, key, value);
            else node.value = value;

            return balance(node);
        }

        private static <Key extends Comparable<Key>, Value> Node<Key, Value>
        balance(Node<Key, Value> node) {
            if (red(node.right) && !red(node.left)) node = rotateLeft(node);
            if (red(node.left) && red(node.left.left)) node = rotateRight(node);
            if (red(node.right) && red(node.left)) flip(node);

            // update ranking
            node.size = 1 + size(node.left) + size(node.right);

            return node;
        }

        private static <Key extends Comparable<Key>, Value> Node<Key, Value>
        rotateLeft(Node<Key, Value> node) {
            assert node != null;
            assert red(node.right) && !red(node.left);

            final Node<Key, Value> right = node.right;
            node.right = right.left;
            right.left = node;
            right.color = node.color;
            node.color = RED;
            right.size = node.size;
            node.size = 1 + size(node.left) + size(node.right);
            return right;
        }

        private static <Key extends Comparable<Key>, Value> Node<Key, Value>
        rotateRight(Node<Key, Value> node) {
            assert node != null;
            assert red(node.left) && red(node.left.left);

            final Node<Key, Value> left = node.left;
            node.left = left.right;
            left.right = node;
            left.color = node.color;
            node.color = RED;
            left.size = node.size;
            node.size = 1 + size(node.left) + size(node.right);
            return left;
        }

        private static void
        flip(Node<?, ?> node) {
            assert node != null;
            assert red(node.left) && red(node.right);

            node.color = !node.color;
            node.left.color = !node.left.color;
            node.right.color = !node.right.color;
        }

        private static <Key extends Comparable<Key>, Value> Value
        get(Node<Key, Value> root, Key key) {
            assert key != null;

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
