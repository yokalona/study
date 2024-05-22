package com.yokalona.array.lazy;

abstract class Primitive<Type> implements Serializable, FixedSizeObject {
    abstract Type value();
}
