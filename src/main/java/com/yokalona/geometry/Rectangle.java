package com.yokalona.geometry;

import java.util.Objects;

public class Rectangle implements Field {

    public static boolean XMAX = true;
    public static boolean YMAX = true;
    public static boolean XMIN = false;
    public static boolean YMIN = false;
    public static Field INFINITELY_LARGE_PLANE = new Rectangle(Double.NEGATIVE_INFINITY,
            Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    public static Field INFINITELY_SMALL_PLANE = new Rectangle(Double.NEGATIVE_INFINITY,
            Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);

    private final double xmin, ymin;
    private final double xmax, ymax;

    public Rectangle(double xmin, double ymin, double xmax, double ymax) {
        this.xmin = Math.min(xmin, xmax);
        this.ymin = Math.min(ymin, ymax);
        this.xmax = Math.max(xmin, xmax);
        this.ymax = Math.max(ymin, ymax);
    }

    public Rectangle(Point leftBottom, Point rightUpper) {
        this(leftBottom.x(), leftBottom.y(), rightUpper.x(), rightUpper.y());
    }

    public Rectangle(Point center, double halfLength) {
        this(center.x() - halfLength, center.y() - halfLength, center.x() + halfLength, center.y() + halfLength);
    }

    public Rectangle(Point center, double xHalfLength, double yHalfLength) {
        this(center.x() - xHalfLength, center.y() - yHalfLength, center.x() + xHalfLength, center.y() + yHalfLength);
    }

    public Rectangle(double xmin, double ymin, Point rightUpper) {
        this(xmin, ymin, rightUpper.x(), rightUpper.y());
    }

    @Override
    public Point
    center() {
        return new Point((xmax - xmin) / 2, (ymax - ymin) / 2);
    }

    public Point
    point(boolean xAxis, boolean yAxis) {
        return new Point(xAxis ? xmax : xmin, yAxis ? ymax : ymin);
    }

    @Override
    public boolean
    finite() {
        return Field.isMeasureFinite(xmin) && Field.isMeasureFinite(ymin) && Field.isMeasureFinite(xmax) && Field.isMeasureFinite(ymax);
    }

    @Override
    public boolean
    contains(Point point) {
        return point.x() >= xmin && point.x() <= xmax && point.y() >= ymin && point.y() <= ymax;
    }

    @Override
    public double
    distance(Point point) {
        return Math.sqrt(distanceSquared(point));
    }

    @Override
    public double
    distanceSquared(Point point) {
        double dx = .0;
        double dy = .0;

        if (point.x() < xmin) dx = point.x() - xmin;
        else if (point.x() > xmax) dx = point.x() - xmax;
        if (point.y() < ymin) dy = point.y() - ymin;
        else if (point.y() > ymax) dy = point.y() - ymax;

        return dx * dx + dy * dy;
    }

    @Override
    public double distanceSquaredToCenter(Point point) {
        return center().distanceSquared(point);
    }

    @Override
    public boolean
    onTheLeftOf(final Point point, final boolean axis) {
        if (axis) return xmin <= point.x();
        return ymin <= point.y();
    }

    @Override
    public boolean
    onTheRightOf(final Point point, final boolean axis) {
        if (axis) return xmax >= point.x();
        return ymax >= point.y();
    }

    private boolean
    isSquare() {
        return xmax - xmin == ymax - ymin;
    }

    public double
    ymin() {
        return ymin;
    }

    public double
    ymax() {
        return ymax;
    }

    public double
    xmin() {
        return xmin;
    }

    public double
    xmax() {
        return xmax;
    }

    public String
    toString() {
        return "%1s [%s %s]".formatted(xmax - xmin > ymax - ymin ? "▭" : isSquare() ? "□" : "▯",
                new Point(xmin, ymin), new Point(xmax, ymax));
    }

    @Override
    public boolean
    equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rectangle rectangle = (Rectangle) o;
        return Double.compare(xmin, rectangle.xmin) == 0 && Double.compare(ymin, rectangle.ymin) == 0 && Double.compare(xmax, rectangle.xmax) == 0 && Double.compare(ymax, rectangle.ymax) == 0;
    }

    @Override
    public int
    hashCode() {
        return Objects.hash(xmin, ymin, xmax, ymax);
    }
}
