package com.mcneilio.orcmaker.utils;

import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;

public class Statsd {
    private static StatsDClient instance = null;

    public static void createInstance(String prefix, String hostname, int port, int aggregationFlushInterval) {
        instance = new NonBlockingStatsDClientBuilder()
            .prefix(prefix)
            .hostname(hostname)
            .port(port)
            .aggregationFlushInterval(aggregationFlushInterval)
            .build();
    }

    public static StatsDClient getInstance() {
        if (instance == null)
            throw new RuntimeException("Statsd instance not created");

        return instance;
    }
}
