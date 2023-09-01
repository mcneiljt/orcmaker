package com.mcneilio.orcmaker.metrics.strategies;

import com.mcneilio.orcmaker.metrics.MetricsStrategy;
import com.timgroup.statsd.StatsDClient;

public class StatsDMetricsStrategy implements MetricsStrategy {

  private final StatsDClient client;

  public StatsDMetricsStrategy(StatsDClient client) {
    this.client = client;
  }

  @Override
  public void sendCount(String aspect, long delta, String... tags) {
    client.count(aspect, delta, tags);
  }

  @Override
  public void sendHistogram(String aspect, long value, String... tags) {
    client.histogram(aspect, value, tags);
  }
}
