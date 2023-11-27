package com.yokalona.tree;

import com.yokalona.geometry.Distance;
import com.yokalona.geometry.Field;
import com.yokalona.geometry.Point;
import com.yokalona.geometry.Rectangle;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.yokalona.geometry.Field.X_AXIS;
import static com.yokalona.geometry.Rectangle.*;

public class TwoDimensionalTree {

    private Node root;
    private double xmin = Double.POSITIVE_INFINITY;
    private double xmax = Double.NEGATIVE_INFINITY;
    private double ymin = Double.POSITIVE_INFINITY;
    private double ymax = Double.NEGATIVE_INFINITY;

    public void
    put(final Point point) {
        if (point == null) throw new IllegalArgumentException("Trying to insert null-point");
        if (!point.finite()) throw new IllegalArgumentException("Trying to insert non existing point");
        final double x = point.x();
        final double y = point.y();
        this.xmin = Math.min(xmin, x);
        this.xmax = Math.max(xmax, x);
        this.ymin = Math.min(ymin, y);
        this.ymax = Math.max(ymax, y);
        root = Node.put(root, X_AXIS, point, 0);
    }

    public void
    putAll(final Point[] points) {
        if (points == null) throw new IllegalArgumentException("Trying to insert null-array of points");
        for (Point point : points) {
            this.put(point);
        }
    }

    public void
    putAll(final Collection<Point> points) {
        if (points == null) throw new IllegalArgumentException("Trying to insert null-array of points");
        putAll(points.toArray(new Point[0]));
    }

    public boolean
    contains(final Point point) {
        if (point == null) throw new IllegalArgumentException("Trying to search null-point");
        return Node.contains(root, X_AXIS, point);
    }

    public Set<Point>
    onField(final Field field) {
        if (field == null) throw new IllegalArgumentException("Trying to search on null-field");
        return onField(field, field.pointComparator());
    }

    public Set<Point>
    onField(final Field field, final Comparator<Point> comparator) {
        if (field == null) throw new IllegalArgumentException("Trying to search on null-field");
        if (comparator == null) throw new IllegalArgumentException("Trying to compare with null-comparator");
        return Node.onField(root, field, comparator, ignore -> true);
    }

    public Set<Point>
    onField(final Field field, final Predicate<Point> filter) {
        if (field == null) throw new IllegalArgumentException("Trying to search on null-field");
        return Node.onField(root, field, field.pointComparator(), filter);
    }

    public Set<Point>
    onField(final Field field, final Comparator<Point> comparator, final Predicate<Point> filter) {
        if (field == null) throw new IllegalArgumentException("Trying to search on null-field");
        if (comparator == null) throw new IllegalArgumentException("Trying to compare with null-comparator");
        if (filter == null) throw new IllegalArgumentException("Trying to filter with null-filter");
        return Node.onField(root, field, comparator, filter);
    }

    public Distance
    closestTo(final Point point) {
        if (root == null) return Distance.NON_EXISTING;
        if (point == null) return Distance.NON_EXISTING;
        if (!point.finite()) return Distance.NON_EXISTING;
        return Node.closestTo(root, point, new Distance(root.value, point.distanceSquared(root.value)),
                (Rectangle) Rectangle.INFINITELY_LARGE_PLANE);
    }

    public Distance
    furthestFrom(final Point point) {
        if (root == null) return Distance.NON_EXISTING;
        if (point == null) return Distance.NON_EXISTING;
        if (!point.finite()) return Distance.NON_EXISTING;
        return Node.furthestFrom(root, point, new Distance(root.value, point.distanceSquared(root.value)),
                (Rectangle) Rectangle.INFINITELY_LARGE_PLANE);
    }

    public Rectangle
    plane() {
        return new Rectangle(xmin, ymin, xmax, ymax);
    }

    public int
    size() {
        return Node.size(root);
    }

    public Set<? extends Point>
    toSet() {
        return Collections.unmodifiableSet(toSet(new HashSet<>()));
    }

    public Set<? extends Point>
    toSet(final Set<Point> set) {
        if (set == null) throw new IllegalArgumentException("Trying to insert to null-set");
        return Collections.unmodifiableSet(Node.toSet(root, set));
    }

    public void
    traverse(final Consumer<Point> action) {
        if (action == null) throw new IllegalArgumentException("Trying to do null-action");
        Node.traversePointsBFS(root, action);
    }

    public int depth() {
        if (root == null) return 0;
        return Node.depth(root);
    }

    public void clear() {
        root = null;
    }

    public String
    toString() {
        return Node.toString(root);
    }

    private static class Node {
        private final int depth;
        private final Point value;
        private final boolean axis;

        private int size;
        private Node left;
        private Node right;

        public Node(final Point value, final int depth, final boolean axis, int size) {
            this.size = size;
            this.value = value;
            this.depth = depth;
            this.axis = axis;
        }

        public static Node
        put(final Node node, final boolean coordinate, final Point point, final int depth) {
            if (node == null) return new Node(point, depth, coordinate, 1);
            if (node.value.equals(point)) return node;

            if (compare(node, point) > 0)
                node.left = Node.put(node.left, !coordinate, point, depth + 1);
            else node.right = Node.put(node.right, !coordinate, point, depth + 1);

            node.size = 1 + size(node.left) + size(node.right);

            return node;
        }

        public static boolean
        contains(final Node node, final boolean coordinate, final Point point) {
            if (node == null) return false;
            if (node.value.equals(point)) return true;

            if (compare(node, point) > 0)
                return Node.contains(node.left, !coordinate, point);
            else return Node.contains(node.right, !coordinate, point);
        }

        private static int compare(Node node, Point point) {
            return node.axis
                    ? Double.compare(node.value.x(), point.x())
                    : Double.compare(node.value.y(), point.y());
        }

        private static int
        size(final Node node) {
            if (node == null) return 0;
            return node.size;
        }

        public static Set<Point>
        onField(final Node node, final Field field, final Comparator<Point> comparator, final Predicate<Point> filter) {
            final Set<Point> inRange = new TreeSet<>(comparator);
            return onField(node, field, inRange, filter);
        }

        private static Set<Point>
        onField(Node node, final Field field, final Set<Point> inRange, final Predicate<Point> filter) {
            final LinkedList<Node> nodes = new LinkedList<>();
            while (node != null) {
                if (filter.test(node.value) && field.contains(node.value)) inRange.add(node.value);

                if (node.left != null && field.onTheLeftOf(node.value, node.axis)) nodes.push(node.left);
                if (node.right != null && field.onTheRightOf(node.value, node.axis)) nodes.push(node.right);

                node = nodes.poll();
            }
            return Collections.unmodifiableSet(inRange);
        }

        private static Distance
        closestTo(final Node node, final Point point, final Distance soFar, final Rectangle area) {
            if (node == null) return soFar;
            if (area.distanceSquared(point) >= soFar.distanceSquared()) return soFar;
            return findNearest(node.axis, node, point, soFar, rightArea(node, area), leftArea(node, area));
        }

        private static Distance
        furthestFrom(final Node node, final Point point, final Distance soFar, final Rectangle area) {
            if (node == null) return soFar;
            return findFurthest(node.axis, node, point, soFar, rightArea(node, area), leftArea(node, area));
        }

        private static Rectangle
        leftArea(final Node node, final Rectangle area) {
            return new Rectangle(area.point(XMIN, YMIN), getLeft(node, area));
        }

        private static Rectangle
        rightArea(final Node node, final Rectangle area) {
            return new Rectangle(getRight(node, area), area.point(XMAX, YMAX));
        }

        private static Point
        getLeft(final Node node, final Rectangle area) {
            return node.axis ? new Point(node.value.x(), area.ymax())
                    : new Point(area.xmax(), node.value.y());
        }

        private static Point
        getRight(final Node node, final Rectangle area) {
            return node.axis ? new Point(node.value.x(), area.ymin())
                    : new Point(area.xmin(), node.value.y());
        }

        private static Distance
        findFurthest(final boolean axis, final Node node, final Point point, final Distance soFar, final Rectangle rightArea, final Rectangle leftArea) {
            if (point.onTheRightOf(node.value, axis))
                return furthestFromWithin(node.right, point, furthestFromWithin(node.left, point, soFar, leftArea), rightArea);
            else
                return furthestFromWithin(node.left, point, furthestFromWithin(node.right, point, soFar, rightArea), leftArea);
        }

        private static Distance
        findNearest(final boolean axis, final Node node, final Point point, final Distance soFar, final Rectangle rightArea, final Rectangle leftArea) {
            if (point.onTheRightOf(node.value, axis))
                return closestToWithin(node.left, point, closestToWithin(node.right, point, soFar, rightArea), leftArea);
            else
                return closestToWithin(node.right, point, closestToWithin(node.left, point, soFar, leftArea), rightArea);
        }

        private static Distance
        closestToWithin(final Node question, final Point key, Distance soFar, final Rectangle area) {
            final double distanceSquared = distanceSquared(question, key);
            if (distanceSquared < soFar.distanceSquared())
                soFar = new Distance(question.value, distanceSquared);
            return closestTo(question, key, soFar, area);
        }

        private static Distance
        furthestFromWithin(final Node question, final Point key, Distance soFar, final Rectangle area) {
            if (question == null) return soFar;
            final double distanceSquared = distanceSquared(question, key);
            if (distanceSquared > soFar.distanceSquared())
                soFar = new Distance(question.value, distanceSquared);
            return furthestFrom(question, key, soFar, area);
        }

        private static double
        distanceSquared(final Node root, final Point key) {
            if (root == null) return Double.POSITIVE_INFINITY;
            return root.value.distanceSquared(key);
        }

        private static Set<Point>
        toSet(Node node, Set<Point> set) {
            traverseDFS(node, point -> set.add(point.value));
            return set;
        }

        public static void
        traversePointsDFS(Node node, Consumer<Point> action) {
            traverseDFS(node, point -> action.accept(point.value));
        }

        public static void
        traversePointsBFS(Node node, Consumer<Point> action) {
            traverseBFS(node, point -> action.accept(point.value));
        }

        public static void
        traverseBFS(Node node, Consumer<Node> action) {
            LinkedList<Node> nodes = new LinkedList<>();
            nodes.push(node);
            while (!nodes.isEmpty()) {
                LinkedList<Node> temp = new LinkedList<>();
                while (!nodes.isEmpty()) {
                    node = nodes.poll();
                    action.accept(node);
                    if (node.right != null) temp.push(node.right);
                    if (node.left != null) temp.push(node.left);
                }
                nodes = temp;
            }
        }

        public static void
        traverseDFS(Node node, Consumer<Node> action) {
            final LinkedList<Node> nodes = new LinkedList<>();
            while (node != null) {
                action.accept(node);

                if (node.right != null) nodes.push(node.right);
                if (node.left != null) nodes.push(node.left);

                node = nodes.poll();
            }
        }

        private static int depth(Node node) {
            if (node == null) return -1;
            return 1 + Math.max(depth(node.left), depth(node.right));
        }

        public String
        toString() {
            return "%1s %s".formatted(axis ? "↔" : "↕", value);
        }

        private static String
        toString(Node node) {
            final StringBuilder sb = new StringBuilder();
            traverseDFS(node, point -> {
                sb.repeat("  ", point.depth);
                sb.append(point).append('\n');
            });
            return sb.toString();
        }
    }

}
