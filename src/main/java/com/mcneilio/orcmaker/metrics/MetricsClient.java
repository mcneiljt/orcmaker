package com.mcneilio.orcmaker.metrics;

import com.mcneilio.orcmaker.metrics.strategies.NullMetricsStrategy;
import com.mcneilio.orcmaker.metrics.strategies.StatsDMetricsStrategy;
import com.mcneilio.orcmaker.utils.Statsd;
import com.timgroup.statsd.StatsDClient;

import java.util.Properties;

public class MetricsClient {

  MetricsStrategy strategy;

  public MetricsClient(Properties config) {
    String metricsEnabled = config.getProperty("metrics.enabled");

    if (metricsEnabled != null && metricsEnabled.equalsIgnoreCase("true")) {
      this.strategy = getStrategy(config);
    } else {
      this.strategy = new NullMetricsStrategy();
    }
  }

  public void sendCount(String aspect, long delta, String... tags) {
    strategy.sendCount(aspect, delta, tags);
  }

  public void sendHistogram(String aspect, long value, String... tags) {
    strategy.sendHistogram(aspect, value, tags);
  }

  private MetricsStrategy getStrategy(Properties config) {
    String metricsType = config.getProperty("metrics.type");

    // Statsd support
    if (metricsType != null && metricsType.equalsIgnoreCase("statsd")) {
      Statsd.createInstance(config);
      StatsDClient statsDClient = Statsd.getInstance();
      return new StatsDMetricsStrategy(statsDClient);
    }

    // Fall back to no metrics
    System.err.println("Metrics type (" + metricsType + ") not supported. Metrics disabled.");
    return new NullMetricsStrategy();
  }
}