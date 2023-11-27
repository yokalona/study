package com.yokalona.geometry;

import java.util.Objects;

public class Point implements Field, Location {
    public static final Point INFINITELY_REMOTE_POINT = new Point(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    public static final Point INFINTELY_CLOSE_POINT = new Point(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    public static final Point NON_EXISTING_POINT = new Point(Double.NaN, Double.NaN);
    private final double x, y;

    public Point(final double x, final double y) {
        this.x = x;
        this.y = y;
    }

    public double
    x() {
        return x;
    }

    public double
    y() {
        return y;
    }

    @Override
    public boolean
    finite() {
        return Field.isMeasureFinite(x) && Field.isMeasureFinite(y);
    }

    @Override
    public boolean
    contains(final Point point) {
        return this.equals(point);
    }

    public double
    distance(final Point another) {
        return distance(another.x, another.y);
    }


    public double
    distanceSquared(final Point another) {
        return distanceSquared(another.x, another.y);
    }

    @Override
    public double distanceSquaredToCenter(Point point) {
        return distanceSquared(point);
    }

    @Override
    public boolean
    onTheLeftOf(final Point point, final boolean axis) {
        if (axis) return point.x <= point.x;
        return point.y <= point.y;
    }

    @Override
    public boolean
    onTheRightOf(final Point point, final boolean axis) {
        if (axis) return point.x >= point.x;
        return point.y >= point.y;
    }

    @Override
    public Point
    center() {
        return this;
    }

    public double
    distance(final double xShaft, final double yShaft) {
        return Math.sqrt(distanceSquared(xShaft, yShaft));
    }

    public double
    distanceSquared(final double xShaft, final double yShaft) {
        final double dx = x - xShaft;
        final double dy = y - yShaft;
        return dx * dx + dy * dy;
    }

    @Override
    public boolean
    equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return Double.compare(x, point.x) == 0 && Double.compare(y, point.y) == 0;
    }

    @Override
    public int
    hashCode() {
        return Objects.hash(x, y);
    }

    public String
    toString() {
        return "• [%f, %f]".formatted(x, y);
    }
}
