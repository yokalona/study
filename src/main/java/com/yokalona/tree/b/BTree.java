package com.yokalona.tree.b;

import com.yokalona.tree.Tree;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import static com.yokalona.Validations.CAPACITY_SHOULD_BE_EVEN;
import static com.yokalona.Validations.CAPACITY_SHOULD_BE_GREATER_THAN_2;
import static com.yokalona.Validations.EMPTY_NODE;
import static com.yokalona.Validations.EXCEEDING_CAPACITY;
import static com.yokalona.Validations.HEIGHT_CAN_NOT_BE_NEGATIVE;
import static com.yokalona.Validations.KEY_SHOULD_HAVE_NON_NULL_VALUE;
import static com.yokalona.Validations.validateCapacity;
import static com.yokalona.Validations.validateKey;

public class BTree<Key extends Comparable<Key>, Value>
        implements Tree<Key, Value>, Iterable<Map<Key, Value>> {
    private static final String INDENT = "     ";
    private final int capacity;
    private Node<Key, Value> root;
    private int height = 0;
    private int size = 0;

    public BTree(final int capacity) {
        validateCapacity(capacity);
        this.capacity = capacity;
        this.root = new Node<>(capacity);
    }

    @Override
    public Value
    get(final Key key) {
        validateKey(key);
        final Leaf<?, Value> leaf = root.get.by(key, height);
        if (leaf == null) return null;
        return leaf.value;
    }

    @Override
    public boolean
    contains(final Key key) {
        validateKey(key);
        return root.get.by(key, height) != null;
    }

    @Override
    public boolean
    insert(final Key key, final Value value) {
        validateKey(key);
        Node.Insert.Result<Key, Value> result = root.insert.by(key, value, height);
//        assert isBTree();
        if (!result.inserted) return false;
        size++;
        if (result.node == null) return true;
        final Node<Key, Value> split = new Node<>(capacity);
        split.children.insert(Leaf.internal(root.get.min().key, root));
        split.children.insert(Leaf.internal(result.node.get.min().key, result.node));
        root = split;
        height++;
        return true;
    }

    @Override
    public boolean
    remove(final Key key) {
        validateKey(key);
        boolean removed = root.remove.by(key, height);
        assert isBTree();
        if (!removed) return false;
        size--;
        if (root.children.size() == 1 && root.get.min().next != null) {
            root = root.get.min().next;
            height--;
        }
        return true;
    }

    @Override
    public int
    height() {
        return height;
    }

    @Override
    public int
    size() {
        return size;
    }

    @Override
    public void
    clear() {
        size = 0;
        height = 0;
        root = new Node<>(capacity);
    }

    public Entry<Key, Value>
    min() {
        if (root.children.size() == 0) return null;
        Node<Key, Value> node = root;
        while (node.get.min().next != null) node = node.get.min().next;
        return Entry.fromLeaf(node.get.min());
    }

    public Entry<Key, Value>
    max() {
        if (root.children.size() == 0) return null;
        Node<Key, Value> node = root;
        while (node.get.max().next != null) node = node.get.max().next;
        return Entry.fromLeaf(node.get.max());
    }

    @Override
    public Iterator<Map<Key, Value>>
    iterator() {
        int height = height();
        Queue<Node<Key, Value>> nodes = new LinkedList<>();
        if (root == null) return Collections.emptyIterator();
        nodes.offer(root);
        while (!nodes.isEmpty()) {
            if (height == 0) break;
            int size = nodes.size();
            for (int level = 0; level < size; level++) {
                Node<Key, Value> node = nodes.poll();
                if (node == null) continue;
                for (Leaf<Key, Value> child : node.children) {
                    if (child == null) break;
                    nodes.offer(child.next);
                }
            }
            height--;
        }
        return new BlockIterator<>(nodes.iterator());
    }

    void
    check() {
        assert isBTree();
    }

    private boolean
    isBTree() {
        Queue<Node<?, ?>> nodes = new LinkedList<>();
        nodes.offer(root);
        int height = 0;
        while (!nodes.isEmpty()) {
            int size = nodes.size();
            for (int i = 0; i < size; i++) {
                Node<?, ?> node = nodes.poll();
                assert node != null;
                assert node.children.check();
                for (Leaf<?, ?> leaf : node.children) {
                    assert leaf.key != null;
                    if (height != height()) {
                        assert leaf.value == null;
                        assert leaf.next != null;
                        nodes.add(leaf.next);
                    } else {
                        assert leaf.next == null;
                        assert leaf.value != null;
                    }
                }
            }
            height++;
        }
        return true;
    }

    private static class Node<Key extends Comparable<Key>, Value> {
        private final DataBlock<Key, Leaf<Key, Value>> children;

        private final Get get = new Get();
        private final Rotate rotate = new Rotate();
        private final Insert insert = new Insert();
        private final Remove remove = new Remove();
        private final Sibling sibling = new Sibling();

        @SuppressWarnings("unchecked")
        private Node(final int capacity) {
            assert capacity > 2 : CAPACITY_SHOULD_BE_GREATER_THAN_2;
            assert capacity % 2 == 0 : CAPACITY_SHOULD_BE_EVEN;

            this.children = new DataBlock<>(() -> (Leaf<Key, Value>[]) new Leaf[capacity]);
        }

        private Node<Key, Value>
        split() {
            final Node<Key, Value> split = new Node<>(children.length());
            children.splitWith(split.children);
            return split;
        }

        private void
        balance(final Node<Key, Value> parent, final int index) {
            final Node<Key, Value> left = sibling.left(parent, index);
            final Node<Key, Value> right = sibling.right(parent, index);
            if (right != null && right.haveSpare()) rotate.left(parent, index, right);
            else if (left != null && left.haveSpare()) rotate.right(parent, index, left);
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

        private Map<Key, Value>
        extract() {
            Map<Key, Value> map = new HashMap<>();
            for (int child = 0; child < children.size(); child++) {
                map.put(children.get(child).key, children.get(child).value);
            }
            return map;
        }

        private class Get {

            public Leaf<?, Value>
            by(final Key key, int height) {
                assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
                assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

                Node<Key, Value> next = Node.this;
                while (height > 0) {
                    int node = next.children.find.greaterThan(key) - 1;
                    next = next.children.get(node).next;
                    height --;
                }
                final int child = next.children.find.equal(key);
                if (child < 0 || child >= next.children.size()) return null;
                else return next.children.get(child);
            }

            public Leaf<Key, Value>
            min() {
                assert children.size() > 0 : EMPTY_NODE;

                return children.get(0);
            }

            public Leaf<Key, Value>
            max() {
                assert children.size() > 0 : EMPTY_NODE;

                return children.get(children.size() - 1);
            }

            public Leaf<Key, Value>
            rank(int rank) {
                assert rank >= 0 && rank < children.size() : EXCEEDING_CAPACITY;

                return children.get(rank);
            }

        }

        private class Insert {

            public Result<Key, Value>
            by(final Key key, final Value value, final int height) {
                assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
                assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

                Result<Key, Value> result;
                if (height == 0) result = leaf(key, value);
                else result = node(key, value, height);
                if (result.inserted && children.size() >= children.length()) return result.node(split());
                else return result.node(null);
            }

            private Result<Key, Value>
            leaf(final Key key, final Value value) {
                assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

                int child = children.find.equal(key);
                if (child < 0) {
                    children.insert(-child - 1, Leaf.external(key, value));
                    return new Result<>(true);
                } else {
                    children.replace(child, Leaf.external(key, value));
                    return new Result<>(false);
                }
            }

            private Result<Key, Value>
            node(final Key key, final Value value, final int height) {
                assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
                assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

                int node = children.find.greaterThan(key) - 1;
                if (node < 0) node = 0;
                final Result<Key, Value> result = children.get(node).next.insert.by(key, value, height - 1);
                if (result.inserted && result.node != null) {
                    children.replace(node, Leaf.internal(children.get(node).next.get.min().key, children.get(node).next));
                    children.insert(node + 1, Leaf.internal(result.node.get.min().key, result.node));
                }
                return result.node(null);
            }


            private static class Result<Key extends Comparable<Key>, Value> {
                private final boolean inserted;
                private Node<Key, Value> node;

                private Result(boolean inserted) {
                    this.inserted = inserted;
                }

                private Result<Key, Value>
                node(Node<Key, Value> node) {
                    this.node = node;
                    return this;
                }
            }
        }

        private class Remove {

            public boolean
            by(final Key key, final int height) {
                return by(key, null, -1, height);
            }

            private boolean
            by(final Key key, final Node<Key, Value> parent, final int index, final int height) {
                assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
                assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

                if (height == 0) return leaf(key, parent, index);
                else return node(key, parent, index, height);
            }

            private boolean
            by(final Key key) {
                assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

                final int child = children.find.equal(key);
                if (child < 0) return false;
                children.remove(child);
                return true;
            }

            private boolean
            node(Key key, Node<Key, Value> parent, int index, int height) {
                int node = children.find.greaterThan(key) - 1;
                if (node >= children.size()) return false;
                final boolean removed = children.get(node).next.remove.by(key, Node.this, node, height - 1);
                if (deficient()) balance(parent, index);
                return removed;
            }

            private boolean
            leaf(Key key, Node<Key, Value> parent, int index) {
                final boolean removed = remove.by(key);
                if (deficient()) balance(parent, index);
                return removed;
            }

            private void
            min() {
                children.remove(0);
            }

            private void
            max() {
                children.remove(children.size() - 1);
            }

        }

        private class Rotate {
            private void
            left(Node<Key, Value> parent, int index, Node<Key, Value> right) {
                children.insert(right.get.min());
                parent.children.replace(index + 1, Leaf.internal(right.get.rank(1).key, right));
                right.remove.min();
            }

            private void
            right(Node<Key, Value> parent, int index, Node<Key, Value> left) {
                children.insert(0, left.get.max());
                parent.children.replace(index, Leaf.internal(left.get.max().key, Node.this));
                left.remove.max();
            }
        }

        private class Sibling {
            private Node<Key, Value>
            left(final Node<Key, Value> parent, final int index) {
                if (parent != null && index > 0) return parent.children.get(index - 1).next;
                return null;
            }

            private Node<Key, Value>
            right(final Node<Key, Value> parent, final int index) {
                if (parent != null && index < parent.children.size() - 1) return parent.children.get(index + 1).next;
                return null;
            }
        }
    }

    private record Leaf<Key extends Comparable<Key>, Value>(Key key, Value value, Node<Key, Value> next)
            implements HasKey<Key>, HasValue<Value> {

        public static <Key extends Comparable<Key>, Value> Leaf<Key, Value>
        internal(Key key, Node<Key, Value> next) {
            return new Leaf<>(key, null, next);
        }

        public static <Key extends Comparable<Key>, Value> Leaf<Key, Value>
        external(Key key, Value value) {
            return new Leaf<>(key, value, null);
        }

    }

    private record BlockIterator<Key extends Comparable<Key>, Value>
            (Iterator<Node<Key, Value>> underlying) implements Iterator<Map<Key, Value>> {

        @Override
        public boolean
        hasNext() {
            return underlying.hasNext();
        }

        @Override
        public Map<Key, Value>
        next() {
            Node<Key, Value> next = underlying.next();
            return next.extract();
        }
    }

    public record Entry<Key extends Comparable<Key>, Value>
            (Key key, Value value) implements HasKey<Key> {
        private static <Key extends Comparable<Key>, Value> Entry<Key, Value>
        fromLeaf(Leaf<Key, Value> leaf) {
            return new Entry<>(leaf.key, leaf.value);
        }
    }

    public String
    toString() {
        return toString(root, height, "") + "\n";
    }

    private String
    toString(Node<?, ?> node, int height, String indent) {
        StringBuilder sb = new StringBuilder();
        if (height == 0) printLeaf(node, indent, sb, node.children);
        else printNode(node, height, indent, sb, node.children);
        return sb.toString();
    }

    private void
    printNode(Node<?, ?> node, int height, String indent, StringBuilder sb, DataBlock<?, ? extends Leaf<?, ?>> children) {
        for (int child = 0; child < node.children.size(); child++) {
            if (child > 0 || node.children.size() == 1) sb.append(indent)
                    .append("(").append(children.get(child).key()).append(")\n");
            sb.append(toString(children.get(child).next(), height - 1, indent + INDENT));
        }
    }

    private static void
    printLeaf(Node<?, ?> node, String indent, StringBuilder sb, DataBlock<?, ? extends Leaf<?, ?>> children) {
        for (int child = 0; child < node.children.size(); child++) {
            sb.append(indent)
                    .append(children.get(child).key()).append(" ").append(children.get(child).value()).append("\n");
        }
    }
}
