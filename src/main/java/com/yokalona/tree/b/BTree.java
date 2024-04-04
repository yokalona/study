package com.yokalona.tree.b;

import static com.yokalona.Validations.*;

// TODO: Use SBBSTree/SkipList for children nodes
// TODO: Should forbid non-unique keys
// TODO: Extract interface
// TODO: Implement iterators:
//   1. BLOCK->BLOCK (possible take advantage of LL)
//   2. Order traversal
//   3. Reverse order traversal
// TODO: Prepare to concurrency rework
public class BTree<Key extends Comparable<Key>, Value> {
    private static final String INDENT = "     ";
    private final int capacity;
    private Node<Key, Value> root;
    private int size = 0;
    private int height = 0;

    public BTree(int capacity) {
        validateCapacity(capacity);
        this.capacity = capacity;
        this.root = new Node<>(capacity);
    }

    public Value
    get(Key key) {
        validateKey(key);
        Leaf<?, Value> value = root.get(key, height);
        if (value == null) return null;
        return value.value;
    }

    public void
    insert(Key key, Value value) {
        validateKey(key);
        final Node<Key, Value> inserted = root.insert(key, value, height);
        size++;
        if (inserted == null) return;
        final Node<Key, Value> split = new Node<>(capacity);
        split.add(Leaf.internal(root.first().key, root));
        split.add(Leaf.internal(inserted.first().key, inserted));
        root = split;
        height++;
    }

    public boolean
    contains(Key key) {
        validateKey(key);
        return root.get(key, height) != null;
    }

    public void
    remove(Key key) {
        validateKey(key);
        boolean removed = root.remove(null, 0, key, height);
        if (! removed) return;
        size--;
        if (root.size == 1 && root.first().next != null) {
            root = root.first().next;
            height--;
        }
    }

    public int
    height() {
        return height;
    }

    public int
    size() {
        return size;
    }

    private static class Node<Key extends Comparable<Key>, Value> {
        private final Leaf<Key, Value>[] children;
        private int size = 0;

        @SuppressWarnings("unchecked")
        private Node(int capacity) {
            assert capacity > 2 : CAPACITY_SHOULD_BE_GREATER_THAN_2;
            assert capacity % 2 == 0 : CAPACITY_SHOULD_BE_EVEN;

            this.children = (Leaf<Key, Value>[]) new Leaf[capacity];
        }

        public Leaf<Key, Value>
        first() {
            assert size > 0 : EMPTY_NODE;

            return children[0];
        }

        public Leaf<Key, Value>
        last() {
            assert size > 0 : EMPTY_NODE;

            return children[size - 1];
        }

        public void
        add(Leaf<Key, Value> child) {
            assert child != null : CHILD_SHOULD_HAVE_NON_NULL_VALUE;

            if (size < children.length) children[size++] = child;
            else throw new IllegalArgumentException("Capacity exceeded");
        }

        public Node<Key, Value>
        insert(Key key, Value value, int height) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
            assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

            if (height == 0) insertLeaf(key, value);
            else insertLeaf(key, value, height);
            if (size < children.length) return null;
            else return split();
        }

        private void
        insertLeaf(Key key, Value value) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            int child = 0;
            for (; child < size; child++) {
                if (key.compareTo(children[child].key) < 0) break;
            }
            assignLeaf(child, Leaf.external(key, value));
        }

        private void
        insertLeaf(Key key, Value value, int height) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
            assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

            int child = 0;
            for (; child < size; child++) {
                if (child + 1 == size || key.compareTo(children[child + 1].key) < 0) {
                    final Node<Key, Value> inserted = children[child++].next.insert(key, value, height - 1);
                    if (inserted == null) return;
                    assignLeaf(child, Leaf.internal(inserted.first().key, inserted));
                    return;
                }
            }
        }

        private void
        assignLeaf(int child, Leaf<Key, Value> leaf) {
            assert child < children.length : SELECTED_CHILD_IS_EXCEEDING_MAX_CAPACITY;
            assert child >= 0 : CHILD_CAN_NOT_BE_NEGATIVE;
            assert leaf != null : CHILD_SHOULD_HAVE_NON_NULL_VALUE;

            makeSpace(child);
            children[child] = leaf;
        }

        private void
        makeSpace(int child) {
            assert child < children.length : SELECTED_CHILD_IS_EXCEEDING_MAX_CAPACITY;
            assert child >= 0 : CHILD_CAN_NOT_BE_NEGATIVE;
            assert size < children.length : EXCEEDING_CAPACITY + size;

            System.arraycopy(children, child, children, child + 1, size - child);
            children[child] = null;
            size++;
        }

        private void
        erase(int child) {
            assert child < children.length : SELECTED_CHILD_IS_EXCEEDING_MAX_CAPACITY;
            assert child >= 0 : CHILD_CAN_NOT_BE_NEGATIVE;
            assert size > 0 : EXCEEDING_CAPACITY + size;

            System.arraycopy(children, child + 1, children, child, size - child);
            children[-- size] = null;
        }

        private Node<Key, Value>
        split() {
            final int half = children.length / 2;
            final Node<Key, Value> split = new Node<>(children.length);
            split.size = size = half;
            System.arraycopy(children, half, split.children, 0, half);
            for (int child = half; child < children.length; child++) {
                children[child] = null;
            }
            return split;
        }

        public Leaf<?, Value>
        get(Key key, int height) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
            assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

            if (height == 0) return getFromLeaf(key);
            else return getFromNode(key, height);
        }

        private Leaf<?, Value>
        getFromLeaf(Key key) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            final int child = search(key);
            if (child >= size) return null;
            else if (children[child].key.compareTo(key) == 0) return children[child];
            else return null;
        }

        private Leaf<?, Value>
        getFromNode(Key key, int height) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
            assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

            for (int child = 0; child < size; child++) {
                if (child + 1 == size || key.compareTo(children[child + 1].key) < 0) {
                    return children[child].next.get(key, height - 1);
                }
            }
            return null;
        }

        private int
        search(Key key) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            int left = 0, right = size;
            while (left < right) {
                int mid = left + (right - left) / 2;
                int comparison = children[mid].key.compareTo(key);
                if (comparison >= 0) right = mid;
                else left = mid + 1;
            }
            return left;
        }

        public boolean
        remove(Node<Key, Value> parent, int index, Key key, int height) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;
            assert height >= 0 : HEIGHT_CAN_NOT_BE_NEGATIVE;

            if (height == 0) {
                boolean removed = remove(key);
                if (size < children.length / 2) balance(parent, index);
                return removed;
            } else {
                int child = 0;
                for (; child < size; child++) {
                    if (child + 1 == size || key.compareTo(children[child + 1].key) < 0) {
                        boolean removed = children[child].next.remove(this, child, key, height - 1);
                        if (size < children.length / 2) balance(parent, index);
                        return removed;
                    }
                }
            }
            return false;
        }

        private boolean
        remove(Key key) {
            assert key != null : KEY_SHOULD_HAVE_NON_NULL_VALUE;

            int child = search(key);
            if (child >= size || children[child].key.compareTo(key) != 0) return false;
            erase(child);
            return true;
        }

        private void
        balance(Node<Key, Value> parent, int index) {
            Node<Key, Value> left = leftSibling(parent, index);
            Node<Key, Value> right = rightSibling(parent, index);
            if (right != null && right.size > children.length / 2) {
                children[size++] = right.first();
                parent.children[index + 1] = Leaf.internal(right.children[1].key, right);
                right.erase(0);
            } else if (left != null && left.size > children.length / 2) {
                makeSpace(0);
                children[0] = left.last();
                parent.children[index] = Leaf.internal(left.last().key, this);
                left.erase(left.size - 1);
            } else if (left != null) {
                for (int child = 0; child < size; child++) {
                    left.children[left.size + child] = children[child];
                    children[child] = null;
                }
                left.size += size;
                parent.erase(index);
            } else if (right != null) {
                for (int child = 0; child < right.size; child++) {
                    children[size + child] = right.children[child];
                    right.children[child] = null;
                }
                size += right.size;
                parent.erase(index + 1);
            }
        }

        private Node<Key, Value>
        leftSibling(Node<Key, Value> parent, int index) {
            if (parent != null && index > 0) return parent.children[index - 1].next;
            return null;
        }

        private Node<Key, Value>
        rightSibling(Node<Key, Value> parent, int index) {
            if (parent != null && index < parent.size - 1) return parent.children[index + 1].next;
            return null;
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
