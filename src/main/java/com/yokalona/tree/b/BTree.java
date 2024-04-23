package com.yokalona.tree.b;

import com.yokalona.tree.Tree;

import java.util.*;

import static com.yokalona.Validations.validateCapacity;
import static com.yokalona.Validations.validateKey;

public class BTree<Key extends Comparable<Key>, Value>
        implements Tree<Key, Value>, Iterable<Map<Key, Value>> {
    private static final String INDENT = "     ";
    private final int capacity;
    private Node<Key, Value> root;
    private int height = 0;
    private int size = 0;

    private final transient Loader<Key, Value> loader;

    public BTree(final int capacity) {
        validateCapacity(capacity);
        this.capacity = capacity;
        this.loader = new Loader<>("", capacity);
        this.root = new Node<>(capacity, loader);
    }

    @Override
    public Value
    get(final Key key) {
        validateKey(key);
        return root.getBy(key, height);
    }

    @Override
    public boolean
    contains(final Key key) {
        validateKey(key);
        return root.contains(key, height);
    }

    @Override
    public boolean
    insert(final Key key, final Value value) {
        validateKey(key);
        Node.Result<Key, Value> result = root.insertBy(key, value, height);
        assert isBTree();
        if (! result.inserted()) return false;
        size++;
        if (result.node() == null) return true;
        final Node<Key, Value> split = new Node<>(capacity, loader);
        split.children().insertInternal(0, root.children().minKey(), root);
        split.children().insertInternal(1, result.node().children().minKey(), result.node());
        root = split;
        height++;
        return true;
    }

    @Override
    public boolean
    remove(final Key key) {
        validateKey(key);
        boolean removed = root.removeBy(key, height);
        assert isBTree();
        if (! removed) return false;
        size--;
        if (root.children().size() == 1 && root.children().minLink() != null) {
            root = root.children().minLink();
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
        root = new Node<>(capacity, loader);
    }

    public Entry<Key, Value>
    min() {
        if (root.children().size() == 0) return null;
        Node<Key, Value> node = root;
        while (node.children().minLink() != null) node = node.children().minLink();
        return node.children().minEntry();
    }

    public Entry<Key, Value>
    max() {
        if (root.children().size() == 0) return null;
        Node<Key, Value> node = root;
        while (node.children().maxLink() != null) node = node.children().maxLink();
        return node.children().maxEntry();
    }

    @Override
    public Iterator<Map<Key, Value>>
    iterator() {
        int height = height();
        Queue<Node<Key, Value>> nodes = new LinkedList<>();
        if (root == null) return Collections.emptyIterator();
        nodes.offer(root);
        while (! nodes.isEmpty()) {
            if (height == 0) break;
            int size = nodes.size();
            for (int level = 0; level < size; level++) {
                Node<Key, Value> node = nodes.poll();
                if (node == null) continue;
                for (Leaf<Key, Value> child : node.children()) {
                    if (child == null) break;
                    nodes.offer(child.link());
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
        while (! nodes.isEmpty()) {
            int size = nodes.size();
            for (int i = 0; i < size; i++) {
                Node<?, ?> node = nodes.poll();
                assert node != null;
                assert node.children().check();
                for (Leaf<?, ?> leaf : node.children()) {
                    assert leaf.key() != null;
                    if (height != height()) {
                        assert leaf.value() == null;
                        assert leaf.link() != null;
                        nodes.add(leaf.link());
                    } else {
                        assert leaf.link() == null;
                        assert leaf.value() != null;
                    }
                }
            }
            height++;
        }
        return true;
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

    public String
    toString() {
        return toString(root, height, "") + "\n";
    }

    private String
    toString(Node<?, ?> node, int height, String indent) {
        StringBuilder sb = new StringBuilder();
        if (height == 0) printLeaf(node, indent, sb, node.children());
        else printNode(node, height, indent, sb, node.children());
        return sb.toString();
    }

    private void
    printNode(Node<?, ?> node, int height, String indent, StringBuilder sb, DataBlock<?, ?> children) {
        for (int child = 0; child < node.children().size(); child++) {
            if (child > 0 || node.children().size() == 1) sb.append(indent)
                    .append("(").append(children.key(child)).append(")\n");
            sb.append(toString(children.link(child), height - 1, indent + INDENT));
        }
    }

    private static void
    printLeaf(Node<?, ?> node, String indent, StringBuilder sb, DataBlock<?, ?> children) {
        for (int child = 0; child < node.children().size(); child++) {
            sb.append(indent)
                    .append(children.key(child)).append(" ").append(children.value(child)).append("\n");
        }
    }
}
