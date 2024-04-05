package com.yokalona.tree;

import java.util.*;

import static com.yokalona.Validations.*;

public class LLRBSearchTree<Key extends Comparable<Key>, Value> implements Tree<Key, Value> {

    private RBNode<Key, Value> root;

    public boolean
    insert(Key key, Value value) {
        validateKey(key);
        if (root == null) root = new RBNode<>(key, value, RBNode.BLACK);
        else root = RBNode.insert(root, key, value);
        root.color = RBNode.BLACK;
        return false;
    }

    @Override
    public boolean contains(Key key) {
        validateKey(key);
        if (root == null) return false;
        else return RBNode.get(root, key) != null;
    }

    @Override
    public boolean remove(Key key) {
        validateKey(key);
        if (!contains(key)) return false;

        if (! RBNode.red(root.left) && ! RBNode.red(root.right)) root.color = RBNode.RED;
        root = RBNode.remove(root, key);
        if (size() > 0) root.color = RBNode.BLACK;
        return true;
    }

    public Value
    get(Key key) {
        validateKey(key);
        RBNode<Key, Value> value = RBNode.get(root, key);
        if (value == null) return null;
        return value.value;
    }

    public RBNode<Key, Value>
    max() {
        return max(root);
    }

    private static <Key extends Comparable<Key>, Value> RBNode<Key, Value>
    max(RBNode<Key, Value> root) {
        if (root == null) return null;
        while (root.right != null) {
            root = root.right;
        }
        return root;
    }

    public RBNode<Key, Value>
    min() {
        return min(root);
    }

    private static <Key extends Comparable<Key>, Value> RBNode<Key, Value>
    min(RBNode<Key, Value> root) {
        if (root == null) return null;
        while (root.left != null) {
            root = root.left;
        }
        return root;
    }

    public int
    size() {
        if (root == null) return 0;
        return root.size;
    }

    @Override
    public void clear() {

    }

    public int
    height() {
        if (root == null) return 0;
        return RBNode.height(root);
    }

    public void
    balance() {
        if (root != null) RBNode.balance(root);
    }

    @SuppressWarnings("unchecked")
    public LLRBSearchTree<Key, Value>[] split() {
        assert root.size % 2 == 0;

        int half = root.size / 2;
        int count = 0;
        LLRBSearchTree<Key, Value>[] result = (LLRBSearchTree<Key, Value>[]) new LLRBSearchTree[]
                {new LLRBSearchTree<>(), new LLRBSearchTree<>()};
        Stack<RBNode<Key, Value>> stack = new Stack<>();
        while (root != null || !stack.isEmpty()) {
            while (root != null) {
                stack.push(root);
                root = root.left;
            }
            root = stack.pop();
            if (count < half) result[0].insert(root.key, root.value);
            else result[1].insert(root.key, root.value);
            root = root.right;
        }

        return result;
    }

    public static class RBNode<Key extends Comparable<Key>, Value> {
        private static final boolean RED = true;
        private static final boolean BLACK = false;

        private Key key;

        private int size;
        private Value value;
        private boolean color;
        private RBNode<Key, Value> left;
        private RBNode<Key, Value> right;

        public RBNode(Key key, Value value, boolean color) {
            assert key != null;

            this.key = key;
            this.value = value;
            this.color = color;
            this.size = 1;
        }

        public static <Key extends Comparable<Key>, Value> RBNode<Key, Value>
        remove(RBNode<Key, Value> root, Key key) {
            if (key.compareTo(root.key) < 0) {
                if (!red(root.left) && !red(root.left.left)) root = moveRedLeft(root);
                root.left = remove(root.left, key);
            } else {
                if (red(root.left)) root = rotateRight(root);
                if (key.compareTo(root.key) == 0 && root.right == null) return null;
                if (!red(root.right) && !red(root.right.left)) root = moveRedRight(root);
                if (key.compareTo(root.key) == 0) {
                    RBNode<Key, Value> min = min(root.right);
                    root.key = min.key;
                    root.value = min.value;
                    root.right = removeMin(root.right);
                } else root.right = remove(root.right, key);
            }
            return balance(root);
        }

        private static <Key extends Comparable<Key>, Value> RBNode<Key, Value>
        removeMin(RBNode<Key, Value> root) {
            if (root.left == null) return null;
            if (!red(root.left) && !red(root.left.left)) root = moveRedLeft(root);

            root.left = removeMin(root.left);
            return balance(root);
        }

        private static <Key extends Comparable<Key>, Value> RBNode<Key, Value>
        moveRedRight(RBNode<Key, Value> root) {
            flip(root);
            if (red(root.left.left)) {
                root = rotateRight(root);
                flip(root);
            }
            return root;
        }

        private static <Key extends Comparable<Key>, Value> RBNode<Key, Value>
        moveRedLeft(RBNode<Key, Value> root) {
            flip(root);
            if (red(root.right.left)) {
                root.right = rotateRight(root.right);
                root = rotateLeft(root);
                flip(root);
            }
            return root;
        }

        public Value
        value() {
            return value;
        }

        private static <Key extends Comparable<Key>, Value> RBNode<Key, Value>
        insert(RBNode<Key, Value> node, Key key, Value value) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            if (node == null) return new RBNode<>(key, value, RED);

            final int comparison = key.compareTo(node.key);
            if (comparison < 0) node.left = insert(node.left, key, value);
            else if (comparison > 0) node.right = insert(node.right, key, value);
            else node.value = value;

            return balance(node);
        }

        private static <Key extends Comparable<Key>, Value> RBNode<Key, Value>
        balance(RBNode<Key, Value> root) {
            assert root != null : ROOT_SHOULD_HAVE_NON_NULL_VALUE;

            if (red(root.right) && !red(root.left)) root = rotateLeft(root);
            if (red(root.left) && red(root.left.left)) root = rotateRight(root);
            if (red(root.right) && red(root.left)) flip(root);

            // update ranking
            root.size = 1 + size(root.left) + size(root.right);

            return root;
        }

        private static <Key extends Comparable<Key>, Value> RBNode<Key, Value>
        rotateLeft(RBNode<Key, Value> root) {
            assert root != null : ROOT_SHOULD_HAVE_NON_NULL_VALUE;

            final RBNode<Key, Value> right = root.right;
            root.right = right.left;
            right.left = root;
            right.color = root.color;
            root.color = RED;
            right.size = root.size;
            root.size = 1 + size(root.left) + size(root.right);
            return right;
        }

        private static <Key extends Comparable<Key>, Value> RBNode<Key, Value>
        rotateRight(RBNode<Key, Value> root) {
            assert root != null : ROOT_SHOULD_HAVE_NON_NULL_VALUE;

            final RBNode<Key, Value> left = root.left;
            root.left = left.right;
            left.right = root;
            left.color = root.color;
            root.color = RED;
            left.size = root.size;
            root.size = 1 + size(root.left) + size(root.right);
            return left;
        }

        private static void
        flip(RBNode<?, ?> root) {
            assert root != null : ROOT_SHOULD_HAVE_NON_NULL_VALUE;
//            assert red(root.left) && red(root.right) : INCORRECT_COLORING;

            root.color = !root.color;
            root.left.color = !root.left.color;
            root.right.color = !root.right.color;
        }

        private static <Key extends Comparable<Key>, Value> RBNode<Key, Value>
        get(RBNode<Key, Value> root, Key key) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            while (root != null) {
                final int comparison = root.key.compareTo(key);
                if (comparison > 0) root = root.left;
                else if (comparison < 0) root = root.right;
                else return root;
            }

            return null;
        }

        private static <Value> Value
        value(RBNode<?, Value> root) {
            if (root == null) return null;
            return root.value;
        }

        private static <Key extends Comparable<Key>> Key
        key(RBNode<Key, ?> root) {
            if (root == null) return null;
            return root.key;
        }

        private static boolean
        red(RBNode<?, ?> node) {
            if (node == null) return BLACK;
            return node.color;
        }

        private static int
        size(RBNode<?, ?> node) {
            if (node == null) return 0;
            return node.size;
        }

        private static int
        height(RBNode<?, ?> node) {
            if (node == null) return -1;
            return 1 + Math.max(height(node.left), height(node.right));
        }

    }

}
