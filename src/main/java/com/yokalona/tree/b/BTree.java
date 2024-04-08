package com.yokalona.tree.b;

import com.yokalona.tree.Tree;

import java.util.*;

import static com.yokalona.Validations.*;

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
        final Leaf<?, Value> value = root.get(key, height);
        if (value == null) return null;
        return value.value;
    }

    @Override
    public boolean
    insert(final Key key, final Value value) {
        if (contains(key)) return false;
        final Node<Key, Value> inserted = root.insert(key, value, height);
        size++;
        if (inserted == null) return true;
        final Node<Key, Value> split = new Node<>(capacity);
        split.children.insert(Leaf.internal(root.min().key, root));
        split.children.insert(Leaf.internal(inserted.min().key, inserted));
        root = split;
        height++;
        return true;
    }

    @Override
    public boolean
    contains(final Key key) {
        validateKey(key);
        return root.get(key, height) != null;
    }

    @Override
    public boolean
    remove(final Key key) {
        validateKey(key);
        boolean removed = root.remove(null, 0, key, height);
        if (!removed) return false;
        size--;
        if (root.children.size() == 1 && root.min().next != null) {
            root = root.min().next;
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
        while (node.min().next != null) node = node.min().next;
        return Entry.fromLeaf(node.min());
    }

    public Entry<Key, Value>
    max() {
        if (root.children.size() == 0) return null;
        Node<Key, Value> node = root;
        while (node.max().next != null) node = node.max().next;
        return Entry.fromLeaf(node.max());
    }

    @Override
    public Iterator<Map<Key, Value>> iterator() {
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

    void check() {
        assert isBTree();
    }

    private boolean isBTree() {
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

        @SuppressWarnings("unchecked")
        private Node(final int capacity) {
            assert capacity > 2 : CAPACITY_SHOULD_BE_GREATER_THAN_2;
            assert capacity % 2 == 0 : CAPACITY_SHOULD_BE_EVEN;

            this.children = new DataBlock<>(() -> (Leaf<Key, Value>[]) new Leaf[capacity]);
        }

        public Leaf<?, Value>
        get(final Key key, final int height) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
            assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

            if (height == 0) return getFromLeaf(key);
            else return getFromNode(key, height);
        }

        public Node<Key, Value>
        insert(final Key key, final Value value, final int height) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
            assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

            if (height == 0) insertLeaf(key, value);
            else insertNode(key, value, height);
            if (children.size() < children.length()) return null;
            else return split();
        }

        public boolean
        remove(final Node<Key, Value> parent, final int index, final Key key, final int height) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
            assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

            if (height == 0) return removeFromLeaf(parent, index, key);
            else return removeFromNode(parent, index, key, height);
        }

        private boolean removeFromNode(Node<Key, Value> parent, int index, Key key, int height) {
            int node = children.find.greaterThan(key) - 1;
            if (node >= children.size()) return false;
            final boolean removed = children.get(node).next.remove(this, node, key, height - 1);
            if (deficient()) balance(parent, index);
            return removed;
        }

        private boolean removeFromLeaf(Node<Key, Value> parent, int index, Key key) {
            final boolean removed = remove(key);
            if (deficient()) balance(parent, index);
            return removed;
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

        private Leaf<?, Value>
        getFromLeaf(final Key key) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            final int child = children.find.equal(key);
            if (child < 0 || child >= children.size()) return null;
            else return children.get(child);
        }

        private Leaf<?, Value>
        getFromNode(final Key key, final int height) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
            assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

            int node = children.find.greaterThan(key) - 1;
            return children.get(node).next.get(key, height - 1);
        }

        private void
        insertLeaf(final Key key, final Value value) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            int child = children.find.position(key);
            if (child < 0) child *= -1;
            children.insert(child, Leaf.external(key, value));
        }

        private void
        insertNode(final Key key, final Value value, final int height) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
            assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

            int node = children.find.lessThan(key);
            if (node < 0) node = 0;
            final Node<Key, Value> inserted = children.get(node).next.insert(key, value, height - 1);
            if (inserted != null) {
                children.replace(node, Leaf.internal(children.get(node).next.min().key, children.get(node).next));
                children.insert(node + 1, Leaf.internal(inserted.min().key, inserted));
            }
        }

        private void
        removeMin() {
            children.remove(0);
        }

        private void
        removeMax() {
            children.remove(children.size() - 1);
        }

        private Node<Key, Value>
        split() {
            final Node<Key, Value> split = new Node<>(children.length());
            children.splitWith(split.children);
            return split;
        }

        private boolean
        remove(final Key key) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            final int child = children.find.equal(key);
            if (child < 0) return false;
            children.remove(child);
            return true;
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
        rotateLeft(Node<Key, Value> parent, int index, Node<Key, Value> right) {
            children.insert(right.min());
            parent.children.replace(index + 1, Leaf.internal(right.rank(1).key, right));
            right.removeMin();
        }

        private void
        rotateRight(Node<Key, Value> parent, int index, Node<Key, Value> left) {
            children.insert(0, left.max());
            parent.children.replace(index, Leaf.internal(left.max().key, this));
            left.removeMax();
        }

        private void
        mergeWith(Node<Key, Value> other, Node<Key, Value> parent, int index) {
            children.mergeWith(other.children);
            parent.children.remove(index);
        }

        private Node<Key, Value>
        leftSibling(final Node<Key, Value> parent, final int index) {
            if (parent != null && index > 0) return parent.children.get(index - 1).next;
            return null;
        }

        private Node<Key, Value>
        rightSibling(final Node<Key, Value> parent, final int index) {
            if (parent != null && index < parent.children.size() - 1) return parent.children.get(index + 1).next;
            return null;
        }

        private Map<Key, Value>
        extract() {
            Map<Key, Value> map = new HashMap<>();
            for (int child = 0; child < children.size(); child++) {
                map.put(children.get(child).key, children.get(child).value);
            }
            return map;
        }

    }

    private record Leaf<Key extends Comparable<Key>, Value>(Key key, Value value, Node<Key, Value> next)
            implements WithKey<Key> {

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
        public boolean hasNext() {
            return underlying.hasNext();
        }

        @Override
        public Map<Key, Value> next() {
            Node<Key, Value> next = underlying.next();
            return next.extract();
        }
    }

    public record Entry<Key extends Comparable<Key>, Value>
            (Key key, Value value) implements WithKey<Key> {
        private static <Key extends Comparable<Key>, Value> Entry<Key, Value>
        fromLeaf(Leaf<Key, Value> leaf) {
            return new Entry<>(leaf.key, leaf.value);
        }
    }

    public String toString() {
        return toString(root, height, "") + "\n";
    }

    private String toString(Node<?, ?> node, int height, String indent) {
        StringBuilder sb = new StringBuilder();
        if (height == 0) printLeaf(node, indent, sb, node.children);
        else printNode(node, height, indent, sb, node.children);
        return sb.toString();
    }

    private void printNode(Node<?, ?> node, int height, String indent, StringBuilder sb, DataBlock<?, ? extends Leaf<?, ?>> children) {
        for (int child = 0; child < node.children.size(); child++) {
            if (child > 0 || node.children.size() == 1) sb.append(indent)
                    .append("(").append(children.get(child).key()).append(")\n");
            sb.append(toString(children.get(child).next(), height - 1, indent + INDENT));
        }
    }

    private static void printLeaf(Node<?, ?> node, String indent, StringBuilder sb, DataBlock<?, ? extends Leaf<?, ?>> children) {
        for (int child = 0; child < node.children.size(); child++) {
            sb.append(indent)
                    .append(children.get(child).key()).append(" ").append(children.get(child).value()).append("\n");
        }
    }
}
