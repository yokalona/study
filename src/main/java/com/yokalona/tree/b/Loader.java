package com.yokalona.tree.b;

import com.esotericsoftware.kryo.Kryo;
import org.objenesis.instantiator.ObjectInstantiator;

public record Loader<Key extends Comparable<Key>, Value>(String name, Kryo kryo, int capacity) {

    public Loader(String name, Kryo kryo, int capacity) {
        this.kryo = kryo;
        this.name = name;
        this.capacity = capacity;
        kryo.register(Leaf.class);
        kryo.register(Leaf[].class);
        kryo.register(Node.class).setInstantiator((ObjectInstantiator<Node<Key, Value>>) () -> new Node<>(capacity));
        kryo.register(Node[].class);
        kryo.register(DataBlock.class).setInstantiator((ObjectInstantiator<DataBlock<Key, Value>>) () -> new DataBlock<>(capacity));
        kryo.register(BTree.class).setInstantiator((ObjectInstantiator<BTree<Key, Value>>) () -> new BTree<>(capacity));
    }

    public Loader(String name, int capacity) {
        this(name, new Kryo(), capacity);
    }

    void loadNode() {

    }

}
