package com.yokalona.tree.b;

import com.esotericsoftware.kryo.Kryo;

public record FileConfiguration(String data, String offset, Kryo kryo) {
}
