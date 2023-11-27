package com.yokalona.geometry;

import java.util.Objects;

public class Ring implements Field {

    private final Circle inner;
    private final Circle outer;

    private Ring(Circle inner, Circle outer) {
        this.inner = inner.radius() > outer.radius() ? outer : inner;
        this.outer = inner.radius() > outer.radius() ? inner : outer;
    }

    public Ring(final Point center, final double innerRadius, final double outerRadius) {
        this(new Circle(center, innerRadius), new Circle(center, outerRadius));
    }

    @Override
    public boolean
    finite() {
        return inner.finite() && outer.finite();
    }

    @Override
    public boolean
    contains(final Point point) {
        return outer.contains(point) && !inner.contains(point);
    }

    @Override
    public double
    distance(final Point point) {
        return Math.sqrt(distanceSquared(point));
    }

    @Override
    public double
    distanceSquared(final Point point) {
        if (inner.contains(point))
            return inner.radius() - inner.center().distanceSquared(point);
        return outer.distanceSquared(point);
    }

    @Override
    public double distanceSquaredToCenter(Point point) {
        return inner.distanceSquaredToCenter(point);
    }

    @Override
    public boolean
    onTheLeftOf(final Point point, final boolean axis) {
        return outer.onTheLeftOf(point, axis);
    }

    @Override
    public boolean
    onTheRightOf(final Point point, final boolean axis) {
        return outer.onTheRightOf(point, axis);
    }

    @Override
    public Point
    center() {
        return inner.center();
    }

    public String
    toString() {
        return "⊚ (∘ %f ⭘ %f) %s".formatted(inner.radius(), outer.radius(), inner.center());
    }

    @Override
    public boolean
    equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Ring ring = (Ring) o;
        return Objects.equals(inner, ring.inner) && Objects.equals(outer, ring.outer);
    }

    @Override
    public int
    hashCode() {
        return Objects.hash(inner, outer);
    }
}
