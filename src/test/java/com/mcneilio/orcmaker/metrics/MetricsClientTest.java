package com.mcneilio.orcmaker.metrics;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.*;

import com.mcneilio.orcmaker.metrics.strategies.NullMetricsStrategy;
import com.mcneilio.orcmaker.metrics.strategies.StatsDMetricsStrategy;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class MetricsClientTest {

  @Test
  public void testStatsdMetricsStrategy() {
    Properties config = new Properties();
    config.setProperty("metrics.enabled", "true");
    config.setProperty("metrics.type", "statsd");

    MetricsClient metrics = new MetricsClient(config);

    assertNotNull(metrics);
    assertInstanceOf(StatsDMetricsStrategy.class, metrics.strategy);
  }

  @Test
  public void testNullMetricsStrategy() {
    Properties config = new Properties();
    config.setProperty("metrics.enabled", "false");

    MetricsClient metrics = new MetricsClient(config);

    assertNotNull(metrics);
    assertInstanceOf(NullMetricsStrategy.class, metrics.strategy);
  }

  @Test
  public void testUnsupportedMetricsStrategyDefaultsToNullStrategy() {
    Properties config = new Properties();
    config.setProperty("metrics.enabled", "true");
    config.setProperty("metrics.type", "unsupported");

    MetricsClient metrics = new MetricsClient(config);

    assertNotNull(metrics);
    assertInstanceOf(NullMetricsStrategy.class, metrics.strategy);
  }
}
