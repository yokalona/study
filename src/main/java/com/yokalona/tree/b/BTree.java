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
        validateKey(key);
        if (contains(key)) return false;
        final Node<Key, Value> inserted = root.insert(key, value, height);
        size++;
        if (inserted == null) return true;
        final Node<Key, Value> split = new Node<>(capacity);
        split.insert(Leaf.internal(root.min().key, root));
        split.insert(Leaf.internal(inserted.min().key, inserted));
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
        if (! removed) return false;
        size--;
        if (root.size == 1 && root.min().next != null) {
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
        if (root.size == 0) return null;
        Node<Key, Value> node = root;
        while (node.min().next != null) {
            node = node.min().next;
        }
        return Entry.fromLeaf(node.min());
    }

    public Entry<Key, Value>
    max() {
        if (root.size == 0) return null;
        Node<Key, Value> node = root;
        while (node.max().next != null) {
            node = node.max().next;
        }
        return Entry.fromLeaf(node.max());
    }

    @Override
    public Iterator<Map<Key, Value>> iterator() {
        int height = height();
        Queue<Node<Key, Value>> nodes = new LinkedList<>();
        if (root == null) return Collections.emptyIterator();
        nodes.offer(root);
        while (! nodes.isEmpty()) {
            if (height == 0) break;
            int size = nodes.size();
            for (int level = 0; level < size; level++) {
                Node<Key, Value> node = nodes.poll();
                for (Leaf<Key, Value> child : node.children) {
                    if (child == null) break;
                    nodes.offer(child.next);
                }
            }
            height--;
        }
        return new BlockIterator<>(nodes.iterator());
    }

    void checkOrderConstraint() {
        Queue<Node<Key, Value>> nodes = new LinkedList<>();
        nodes.offer(root);
        int height = 0;
        Key previous = null;
        while (! nodes.isEmpty()) {
            int size = nodes.size();
            for (int i = 0; i < size; i++) {
                Node<Key, Value> node = nodes.poll();
                if (node == null) throw new IllegalStateException("Node can not be null");

                for (int child = 0; child < node.size; child++) {
                    Leaf<Key, Value> leaf = node.children[child];
                    if (height == height()) {
                        if (previous == null || previous.compareTo(leaf.key) <= 0) previous = leaf.key;
                        else throw new IllegalStateException("Order consistency violated");
                    } else {
                        if (leaf.value != null) throw new IllegalStateException("External node on wrong level");
                        nodes.add(leaf.next());
                    }
                }
            }
            height++;
        }
    }

    void checkCapacityConstraint() {
        Queue<Node<?, ?>> nodes = new LinkedList<>();
        nodes.offer(root);
        while (! nodes.isEmpty()) {
            int size = nodes.size();
            for (int i = 0; i < size; i++) {
                Node<?, ?> node = nodes.poll();
                if (node == null) throw new IllegalStateException("Node can not be null");
                else if (node.size >= capacity) throw new IllegalStateException(EXCEEDING_CAPACITY);
                else if (node != root && node.size < capacity / 2) throw new IllegalStateException(EXCEEDING_CAPACITY);
                else if (node.children.length > capacity) throw new IllegalStateException(EXCEEDING_CAPACITY);

                for (Leaf<?, ?> leaf : node.children) {
                    if (leaf != null && leaf.next() != null) {
                        nodes.add(leaf.next());
                    }
                }
            }
        }
    }

    public void check() {
        checkOrderConstraint();
        checkCapacityConstraint();
    }

    private static class Node<Key extends Comparable<Key>, Value> {
        private final Leaf<Key, Value>[] children;
        private int size = 0;

        @SuppressWarnings("unchecked")
        private Node(final int capacity) {
            assert capacity > 2 : CAPACITY_SHOULD_BE_GREATER_THAN_2;
            assert capacity % 2 == 0 : CAPACITY_SHOULD_BE_EVEN;

            this.children = (Leaf<Key, Value>[]) new Leaf[capacity];
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
            else insertLeaf(key, value, height);
            if (size < children.length) return null;
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
            int node = Math.max(search(key) - 1, 0);
            if (node >= size) return false;
            final boolean removed = children[node].next.remove(this, node, key, height - 1);
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
            assert size > 0 : EMPTY_NODE;

            return children[0];
        }

        public Leaf<Key, Value>
        max() {
            assert size > 0 : EMPTY_NODE;

            return children[size - 1];
        }

        public Leaf<Key, Value>
        rank(int rank) {
            assert rank >= 0 && rank < size : EXCEEDING_CAPACITY;

            return children[rank];
        }

        private Leaf<?, Value>
        getFromLeaf(final Key key) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            final int child = search(key) - 1;
            if (child >= size || child < 0) return null;
            else if (children[child].key.compareTo(key) == 0) return children[child];
            else return null;
        }

        private Leaf<?, Value>
        getFromNode(final Key key, final int height) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
            assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

            int node = Math.max(search(key) - 1, 0);
            if (node >= size) return null;
            return children[node].next.get(key, height - 1);
        }

        private void
        insertLeaf(final Key key, final Value value) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            int child = search(key);
            assignLeaf(child, Leaf.external(key, value));
        }

        private void
        insertLeaf(final Key key, final Value value, final int height) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
            assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

            int node = Math.max(search(key) - 1, 0);
            final Node<Key, Value> inserted = children[node++].next.insert(key, value, height - 1);
            if (inserted != null) assignLeaf(node, Leaf.internal(inserted.min().key, inserted));
        }

        private void
        assignLeaf(final int child, final Leaf<Key, Value> leaf) {
            assert child < children.length : SELECTED_CHILD_IS_EXCEEDING_MAX_CAPACITY;
            assert child >= 0 : CHILD_CAN_NOT_BE_NEGATIVE;
            assert leaf != null : CHILD_SHOULD_HAVE_NON_NULL_VALUE;

            makeSpace(child);
            children[child] = leaf;
        }

        private void
        insert(final Leaf<Key, Value> child) {
            assert child != null : CHILD_SHOULD_HAVE_NON_NULL_VALUE;
            assert size < children.length - 1 : EXCEEDING_CAPACITY;

            children[size++] = child;
        }

        private void
        insert(final int position, final Leaf<Key, Value> child) {
            assert child != null : CHILD_SHOULD_HAVE_NON_NULL_VALUE;
            assert size < children.length - 1 : EXCEEDING_CAPACITY;

            makeSpace(position);
            replace(position, child);
        }

        private void
        replace(final int position, final Leaf<Key, Value> child) {
            assert child != null : CHILD_SHOULD_HAVE_NON_NULL_VALUE;

            children[position] = child;
        }

        private void
        makeSpace(final int child) {
            assert child < children.length : SELECTED_CHILD_IS_EXCEEDING_MAX_CAPACITY;
            assert child >= 0 : CHILD_CAN_NOT_BE_NEGATIVE;
            assert size < children.length : EXCEEDING_CAPACITY + size;

            System.arraycopy(children, child, children, child + 1, size - child);
            children[child] = null;
            size++;
        }

        private void
        remove(final int child) {
            assert child < children.length : SELECTED_CHILD_IS_EXCEEDING_MAX_CAPACITY;
            assert child >= 0 : CHILD_CAN_NOT_BE_NEGATIVE;
            assert size > 0 : EXCEEDING_CAPACITY + size;

            System.arraycopy(children, child + 1, children, child, size - child);
            children[-- size] = null;
        }

        private void
        removeMin() {
            remove(0);
        }

        private void
        removeMax() {
            remove(size - 1);
        }

        private Node<Key, Value>
        split() {
            final int half = children.length / 2;
            final Node<Key, Value> split = new Node<>(children.length);
            split.size = size = half;
            System.arraycopy(children, half, split.children, 0, half);
            Arrays.fill(children, half, children.length, null);
            return split;
        }

        private int
        search(final Key key) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            int left = 0, right = size;
            while (left < right) {
                int mid = left + (right - left) / 2;
                int comparison = children[mid].key.compareTo(key);
                if (comparison > 0) right = mid;
                else left = mid + 1;
            }
            return left;
        }

        private boolean
        remove(final Key key) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            final int child = search(key) - 1;
            if (child >= size || child < 0 || children[child].key.compareTo(key) != 0) return false;
            remove(child);
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
            return size > children.length / 2;
        }

        private boolean
        deficient() {
            return size < children.length / 2;
        }

        private void
        rotateLeft(Node<Key, Value> parent, int index, Node<Key, Value> right) {
            insert(right.min());
            parent.replace(index + 1, Leaf.internal(right.rank(1).key, right));
            right.removeMin();
        }

        private void
        rotateRight(Node<Key, Value> parent, int index, Node<Key, Value> left) {
            insert(0, left.max());
            parent.replace(index, Leaf.internal(left.max().key, this));
            left.removeMax();
        }

        private void
        mergeWith(Node<Key, Value> other, Node<Key, Value> parent, int index) {
            if (size >= 0) System.arraycopy(children, 0, other.children, other.size, size);
            other.size += size;
            parent.remove(index);
        }

        private Node<Key, Value>
        leftSibling(final Node<Key, Value> parent, final int index) {
            if (parent != null && index > 0) return parent.children[index - 1].next;
            return null;
        }

        private Node<Key, Value>
        rightSibling(final Node<Key, Value> parent, final int index) {
            if (parent != null && index < parent.size - 1) return parent.children[index + 1].next;
            return null;
        }

        private Map<Key, Value>
        extract() {
            Map<Key, Value> map = new HashMap<>();
            for (int child = 0; child < size; child ++) {
                map.put(children[child].key, children[child].value);
            }
            return map;
        }

    }

    private record Leaf<Key extends Comparable<Key>, Value>(Key key, Value value, Node<Key, Value> next) {

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

    public record Entry<Key extends Comparable<Key>, Value>(Key key, Value value) {
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
        Leaf<?, ?>[] children = node.children;
        if (height == 0) printLeaf(node, indent, sb, children);
        else printNode(node, height, indent, sb, children);
        return sb.toString();
    }

    private void printNode(Node<?, ?> node, int height, String indent, StringBuilder sb, Leaf<?, ?>[] children) {
        for (int child = 0; child < node.size; child++) {
            if (child > 0 || node.size == 1) sb.append(indent)
                    .append("(").append(children[child].key).append(")\n");
            sb.append(toString(children[child].next, height - 1, indent + INDENT));
        }
    }

    private static void printLeaf(Node<?, ?> node, String indent, StringBuilder sb, Leaf<?, ?>[] children) {
        for (int child = 0; child < node.size; child++) {
            sb.append(indent)
                    .append(children[child].key).append(" ").append(children[child].value).append("\n");
        }
    }
}
