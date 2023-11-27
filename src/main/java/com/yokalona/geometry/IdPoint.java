package com.yokalona.geometry;

import java.util.Objects;

public class IdPoint extends Point {

    private final int id;

    public IdPoint(final int id, final double x, final double y) {
        super(x, y);
        this.id = id;
    }

    public int
    id() {
        return id;
    }

    @Override
    public boolean
    equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdPoint idPoint = (IdPoint) o;
        return id == idPoint.id;
    }

    @Override
    public int
    hashCode() {
        return Objects.hash(id);
    }

    public String
    toString() {
        return "%d: %s".formatted(id, super.toString());
    }
}
