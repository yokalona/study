package com.yokalona.array.persitent.serializers;

public record TypeDescriptor<Type>(int size, Class<Type> type) { }
