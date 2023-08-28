package com.mcneilio.orcmaker.metrics.strategies;

import com.mcneilio.orcmaker.metrics.MetricsStrategy;

public class NullMetricsStrategy implements MetricsStrategy {

  public NullMetricsStrategy() {}

  @Override
  public void sendCount(String aspect, long delta, String... tags) {
    // if LOG_LEVEL == DEBUG, output message here?
  }

  @Override
  public void sendHistogram(String aspect, long delta, String... tags) {}
}
