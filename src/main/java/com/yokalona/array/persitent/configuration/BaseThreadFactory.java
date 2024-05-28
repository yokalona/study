package com.yokalona.array.persitent.configuration;

import java.util.concurrent.ThreadFactory;

public record BaseThreadFactory(ThreadGroup group, String name) implements ThreadFactory {

    public BaseThreadFactory(String group, String name) {
        this(new ThreadGroup(group), name);
    }

    @Override
    public Thread newThread(Runnable task) {
        return new Thread(group, task, name);
    }
}
