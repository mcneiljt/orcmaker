package com.mcneilio.orcmaker.metrics;

public interface MetricsStrategy {

  void sendCount(String aspect, long delta, String... tags);
  void sendHistogram(String aspect, long value, String... tags);

}
