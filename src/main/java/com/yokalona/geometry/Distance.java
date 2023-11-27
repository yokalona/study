package com.yokalona.geometry;

import java.util.Objects;

public record Distance(Location point, double distanceSquared) {
    public static Distance INFINITELY_LONG = new Distance(Point.INFINITELY_REMOTE_POINT, Double.POSITIVE_INFINITY);
    public static Distance INFINITELY_SHORT = new Distance(Point.INFINTELY_CLOSE_POINT, Double.NEGATIVE_INFINITY);
    public static Distance NON_EXISTING = new Distance(Point.NON_EXISTING_POINT, Double.NaN);

    public double
    distanceSquared() {
        return distanceSquared;
    }

    public double
    distance() {
        return Math.sqrt(distanceSquared);
    }

    public boolean
    finite() {
        return point.finite() && Field.isMeasureFinite(distanceSquared);
    }

    public String
    toString() {
        return "↦ %s %fu".formatted(point, distance());
    }

    @Override
    public boolean
    equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Distance distance = (Distance) o;
        return Double.compare(distanceSquared, distance.distanceSquared) == 0 && Objects.equals(point, distance.point);
    }

    @Override
    public int
    hashCode() {
        return Objects.hash(point, distanceSquared);
    }
}
