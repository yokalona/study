package com.yokalona.geometry;

import java.util.Comparator;

/**
 * Describes basic field on a Euclidean space
 */
public interface Field {

    boolean X_AXIS = true;
    boolean Y_AXIS = false;

    /**
     * @return if field is actually finite, in other words if all measures of a field are finite
     */
    boolean finite();

    /**
     * Checks if point is within borders of a field.
     * @param point to test
     * @return true if point lies in between or on border of a field
     */
    boolean contains(Point point);

    /**
     * @param point to test
     * @return distanceSquared that point has to travers in order to be included within borders of a field
     */
    double distance(Point point);

    /**
     * @param point to test
     * @return squared distanceSquared that point has to travers in order to be included within borders of a field
     */
    double distanceSquared(Point point);

    /**
     * @param point to test
     * @return squared distanceSquared that point has to travers in order to the center of field
     */
    double distanceSquaredToCenter(Point point);

    /**
     * @param point point
     * @param axis to check
     * @return true if the point is on the left from the field
     */
    boolean onTheLeftOf(Point point, boolean axis);

    /**
     * @param point point
     * @param axis to check
     * @return true if the point is on the right from the field
     */
    boolean onTheRightOf(Point point, boolean axis);

    /**
     * @return center of this field
     */
    Point center();

    /**
     * @return Comparator to compare points in relation to field
     */
    default Comparator<Point> pointComparator() {
        return Comparator.comparingDouble(this::distanceSquaredToCenter)
                .thenComparing(Point::x)
                .thenComparing(Point::y);
    }

    static boolean isMeasureFinite(double measure) {
        return isMeasureCalculable(measure) && Double.NEGATIVE_INFINITY < measure && measure < Double.POSITIVE_INFINITY;
    }

    static boolean isMeasureCalculable(double measure) {
        return measure == measure;
    }
}
