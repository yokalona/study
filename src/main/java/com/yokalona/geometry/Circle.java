package com.yokalona.geometry;

import java.util.Objects;

/**
 * Describes a circle field in Euclidean space.
 */
public class Circle implements Field {

    public static Field INFINITLY_LARGE_PLANE = new Circle(new Point(0, 0), Double.POSITIVE_INFINITY);
    public static Field INFINITLY_SMALL_PLANE = new Circle(new Point(0, 0), Double.NEGATIVE_INFINITY);

    private final Point center;
    private final double radius;

    /**
     * Constructs Circle field from x,y coordinates and radius
     * @param x coordinate
     * @param y coordinate
     * @param radius of circle
     */
    public Circle(final double x, final double y, final double radius) {
        this(new Point(x, y), radius);
    }

    /**
     * Constructs Circle field from x,y coordinates and radius
     * @param center of circle
     * @param radius of circle
     */
    public Circle(final Point center, final double radius) {
        this.center = center;
        this.radius = radius;
    }

    @Override
    public boolean
    finite() {
        return center.finite() && Field.isMeasureFinite(radius);
    }

    @Override
    public boolean
    contains(final Point point) {
        return point.distanceSquared(center) <= radius * radius;
    }

    @Override
    public double
    distance(final Point point) {
        return Math.sqrt(distanceSquared(point));
    }

    @Override
    public double
    distanceSquared(final Point point) {
        if (contains(point)) return 0;
        return point.distanceSquared(center) - radius * radius;
    }

    @Override
    public double distanceSquaredToCenter(Point point) {
        return center.distanceSquared(point);
    }

    @Override
    public boolean
    onTheLeftOf(final Point point, final boolean axis) {
        if (axis) return center.x() - radius <= point.x();
        return center.y() - radius <= point.y();
    }

    @Override
    public boolean
    onTheRightOf(final Point point, final boolean axis) {
        if (axis) return center.x() + radius >= point.x();
        return center.y() + radius >= point.y();
    }

    public Point
    center() {
        return center;
    }

    public double
    radius() {
        return radius;
    }

    public String
    toString() {
        return "∘(%f) %s".formatted(radius, center);
    }

    @Override
    public boolean
    equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Circle circle = (Circle) o;
        return Double.compare(radius, circle.radius) == 0 && Objects.equals(center, circle.center);
    }

    @Override
    public int
    hashCode() {
        return Objects.hash(center, radius);
    }
}
