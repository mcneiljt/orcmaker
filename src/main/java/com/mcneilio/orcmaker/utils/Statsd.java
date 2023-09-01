package com.mcneilio.orcmaker.utils;

import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;

import java.util.Properties;

public class Statsd {

    // These should be set somewhere else; Defaults should not go here.
    private static String DEFAULT_METRICS_PREFIX = "orcmaker";
    private static String DEFAULT_METRICS_HOST = "127.0.0.1";
    private static String DEFAULT_METRICS_PORT = "8125";
    private static String DEFAULT_METRICS_FLUSH_MS = "1000";

    private static StatsDClient instance = null;

    public static void createInstance(String prefix, String hostname, int port, int aggregationFlushInterval) {
        instance = new NonBlockingStatsDClientBuilder()
            .prefix(prefix)
            .hostname(hostname)
            .port(port)
            .aggregationFlushInterval(aggregationFlushInterval)
            .build();
    }

    public static void createInstance(Properties config) {
        String prefix = config.getProperty("metrics.prefix", DEFAULT_METRICS_PREFIX);
        String host = config.getProperty("metrics.host", DEFAULT_METRICS_HOST);
        int port = Integer.parseInt(config.getProperty("metrics.port", DEFAULT_METRICS_PORT));
        int aggregationFlushInterval = Integer.parseInt(config.getProperty("metrics.flushMS", DEFAULT_METRICS_FLUSH_MS));

        instance = new NonBlockingStatsDClientBuilder()
          .prefix(prefix)
          .hostname(host)
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
