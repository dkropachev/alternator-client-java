package com.scylladb.alternator;

/** Configuration for Alternator node health tracking. */
public final class NodeHealthConfig {
  /**
   * Default consecutive DynamoDB traffic failure threshold before an active node is marked down.
   */
  public static final int DEFAULT_CONSECUTIVE_FAILURE_THRESHOLD = 10;

  /** Default consecutive successful down-node probes before a node enters quarantine. */
  public static final int DEFAULT_DOWN_NODE_RECOVERY_SUCCESS_THRESHOLD = 3;

  /** Default consecutive successful-contact threshold before a quarantined node is promoted. */
  public static final int DEFAULT_QUARANTINE_SUCCESS_THRESHOLD = 10;

  /** Default consecutive traffic failure threshold before a quarantined node is marked down. */
  public static final int DEFAULT_QUARANTINE_FAILURE_THRESHOLD = 3;

  /** Default background period for probing down and quarantined nodes. */
  public static final long DEFAULT_DOWN_NODE_PROBE_PERIOD_MS = 30_000;

  /** Default maximum number of concurrent background and explicit health probes. */
  public static final int DEFAULT_HEALTH_PROBE_CONCURRENCY = 4;

  /** Default total timeout for one health probe after it starts running. */
  public static final long DEFAULT_HEALTH_PROBE_TIMEOUT_MS = 5_000;

  /**
   * Legacy default logical-query interval for sampling quarantined nodes.
   *
   * @deprecated Quarantined nodes are now validated by direct control-plane probes.
   */
  @Deprecated public static final int DEFAULT_QUARANTINE_TRAFFIC_INTERVAL = 10;

  /**
   * Legacy client-traffic idle period after which quarantine sampling was armed.
   *
   * @deprecated Quarantined nodes are now validated by direct control-plane probes.
   */
  @Deprecated public static final long DEFAULT_QUARANTINE_TRAFFIC_IDLE_PERIOD_MS = 100;

  private final int consecutiveFailureThreshold;
  private final int downNodeRecoverySuccessThreshold;
  private final int quarantineSuccessThreshold;
  private final int quarantineFailureThreshold;
  private final long downNodeProbePeriodMs;
  private final int healthProbeConcurrency;
  private final long healthProbeTimeoutMs;
  private final int quarantineTrafficInterval;
  private final long quarantineTrafficIdlePeriodMs;
  private final boolean disabled;

  private NodeHealthConfig(
      int consecutiveFailureThreshold,
      int downNodeRecoverySuccessThreshold,
      int quarantineSuccessThreshold,
      int quarantineFailureThreshold,
      long downNodeProbePeriodMs,
      int healthProbeConcurrency,
      long healthProbeTimeoutMs,
      int quarantineTrafficInterval,
      long quarantineTrafficIdlePeriodMs,
      boolean disabled) {
    if (downNodeProbePeriodMs <= 0) {
      throw new IllegalArgumentException(
          "downNodeProbePeriodMs must be positive, but was: " + downNodeProbePeriodMs);
    }
    if (healthProbeConcurrency < 1 || healthProbeConcurrency > 64) {
      throw new IllegalArgumentException(
          "healthProbeConcurrency must be between 1 and 64, but was: " + healthProbeConcurrency);
    }
    if (healthProbeTimeoutMs <= 0) {
      throw new IllegalArgumentException(
          "healthProbeTimeoutMs must be positive, but was: " + healthProbeTimeoutMs);
    }
    this.consecutiveFailureThreshold = Math.max(1, consecutiveFailureThreshold);
    this.downNodeRecoverySuccessThreshold = Math.max(1, downNodeRecoverySuccessThreshold);
    this.quarantineSuccessThreshold = Math.max(1, quarantineSuccessThreshold);
    this.quarantineFailureThreshold = Math.max(1, quarantineFailureThreshold);
    this.downNodeProbePeriodMs = downNodeProbePeriodMs;
    this.healthProbeConcurrency = healthProbeConcurrency;
    this.healthProbeTimeoutMs = healthProbeTimeoutMs;
    this.quarantineTrafficInterval = Math.max(1, quarantineTrafficInterval);
    this.quarantineTrafficIdlePeriodMs = quarantineTrafficIdlePeriodMs;
    this.disabled = disabled;
  }

  /**
   * Returns the default node health configuration.
   *
   * @return default configuration
   */
  public static NodeHealthConfig getDefault() {
    return builder().build();
  }

  /**
   * Returns a configuration with health tracking disabled.
   *
   * @return disabled configuration
   */
  public static NodeHealthConfig disabled() {
    return builder().withDisabled(true).build();
  }

  /**
   * Creates a new builder.
   *
   * @return a builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the consecutive DynamoDB traffic failure threshold for active nodes.
   *
   * @return threshold
   */
  public int getConsecutiveFailureThreshold() {
    return consecutiveFailureThreshold;
  }

  /**
   * Returns the consecutive successful down-node probe threshold for entering quarantine.
   *
   * @return threshold
   */
  public int getDownNodeRecoverySuccessThreshold() {
    return downNodeRecoverySuccessThreshold;
  }

  /**
   * Returns the consecutive successful-contact promotion threshold.
   *
   * @return threshold
   */
  public int getQuarantineSuccessThreshold() {
    return quarantineSuccessThreshold;
  }

  /**
   * Returns the consecutive traffic failure threshold before a quarantined node is marked down.
   *
   * @return threshold
   */
  public int getQuarantineFailureThreshold() {
    return quarantineFailureThreshold;
  }

  /**
   * Returns the background health-probe period for down and quarantined nodes.
   *
   * @return positive probe period in milliseconds
   */
  public long getDownNodeProbePeriodMs() {
    return downNodeProbePeriodMs;
  }

  /**
   * Returns the maximum number of concurrent health probes.
   *
   * @return probe concurrency between 1 and 64
   */
  public int getHealthProbeConcurrency() {
    return healthProbeConcurrency;
  }

  /**
   * Returns the total timeout for one running health probe.
   *
   * @return positive timeout in milliseconds
   */
  public long getHealthProbeTimeoutMs() {
    return healthProbeTimeoutMs;
  }

  /**
   * Returns the retained legacy quarantine-sampling interval.
   *
   * @return legacy logical-query interval; no longer used by routing
   * @deprecated Quarantined nodes are now validated by direct control-plane probes.
   */
  @Deprecated
  public int getQuarantineTrafficInterval() {
    return quarantineTrafficInterval;
  }

  /**
   * Returns the retained legacy quarantine-sampling idle period.
   *
   * @return legacy idle period in milliseconds; no longer used by routing
   * @deprecated Quarantined nodes are now validated by direct control-plane probes.
   */
  @Deprecated
  public long getQuarantineTrafficIdlePeriodMs() {
    return quarantineTrafficIdlePeriodMs;
  }

  /**
   * Returns whether node health tracking is disabled.
   *
   * @return true when disabled
   */
  public boolean isDisabled() {
    return disabled;
  }

  /** Builder for {@link NodeHealthConfig}. */
  public static final class Builder {
    private int consecutiveFailureThreshold = DEFAULT_CONSECUTIVE_FAILURE_THRESHOLD;
    private int downNodeRecoverySuccessThreshold = DEFAULT_DOWN_NODE_RECOVERY_SUCCESS_THRESHOLD;
    private int quarantineSuccessThreshold = DEFAULT_QUARANTINE_SUCCESS_THRESHOLD;
    private int quarantineFailureThreshold = DEFAULT_QUARANTINE_FAILURE_THRESHOLD;
    private long downNodeProbePeriodMs = DEFAULT_DOWN_NODE_PROBE_PERIOD_MS;
    private int healthProbeConcurrency = DEFAULT_HEALTH_PROBE_CONCURRENCY;
    private long healthProbeTimeoutMs = DEFAULT_HEALTH_PROBE_TIMEOUT_MS;
    private int quarantineTrafficInterval = DEFAULT_QUARANTINE_TRAFFIC_INTERVAL;
    private long quarantineTrafficIdlePeriodMs = DEFAULT_QUARANTINE_TRAFFIC_IDLE_PERIOD_MS;
    private boolean disabled = false;

    private Builder() {}

    /**
     * Sets the consecutive DynamoDB traffic failure threshold for active nodes. Values less than
     * one are normalized to one.
     *
     * @param threshold threshold value
     * @return this builder
     */
    public Builder withConsecutiveFailureThreshold(int threshold) {
      this.consecutiveFailureThreshold = threshold;
      return this;
    }

    /**
     * Sets the consecutive successful down-node probe threshold for entering quarantine. Values
     * less than one are normalized to one.
     *
     * @param threshold threshold value
     * @return this builder
     */
    public Builder withDownNodeRecoverySuccessThreshold(int threshold) {
      this.downNodeRecoverySuccessThreshold = threshold;
      return this;
    }

    /**
     * Sets the consecutive successful-contact promotion threshold. Values less than one are
     * normalized to one.
     *
     * @param threshold threshold value
     * @return this builder
     */
    public Builder withQuarantineSuccessThreshold(int threshold) {
      this.quarantineSuccessThreshold = threshold;
      return this;
    }

    /**
     * Sets the consecutive traffic failure threshold before a quarantined node is marked down.
     * Values less than one are normalized to one.
     *
     * @param threshold threshold value
     * @return this builder
     */
    public Builder withQuarantineFailureThreshold(int threshold) {
      this.quarantineFailureThreshold = threshold;
      return this;
    }

    /**
     * Sets the background health-probe period for down and quarantined nodes.
     *
     * @param periodMs period in milliseconds; must be positive
     * @return this builder
     */
    public Builder withDownNodeProbePeriodMs(long periodMs) {
      this.downNodeProbePeriodMs = periodMs;
      return this;
    }

    /**
     * Sets the maximum number of concurrent health probes.
     *
     * @param concurrency probe concurrency between 1 and 64
     * @return this builder
     */
    public Builder withHealthProbeConcurrency(int concurrency) {
      this.healthProbeConcurrency = concurrency;
      return this;
    }

    /**
     * Sets the total timeout for one health probe after it starts running.
     *
     * @param timeoutMs positive timeout in milliseconds
     * @return this builder
     */
    public Builder withHealthProbeTimeoutMs(long timeoutMs) {
      this.healthProbeTimeoutMs = timeoutMs;
      return this;
    }

    /**
     * Retains the legacy quarantine-sampling interval for configuration compatibility. Values less
     * than one are normalized to one.
     *
     * @param interval legacy logical-query interval
     * @return this builder
     * @deprecated Quarantined nodes are now validated by direct control-plane probes.
     */
    @Deprecated
    public Builder withQuarantineTrafficInterval(int interval) {
      this.quarantineTrafficInterval = interval;
      return this;
    }

    /**
     * Retains the legacy quarantine-sampling idle period for configuration compatibility.
     *
     * @param periodMs legacy idle period in milliseconds
     * @return this builder
     * @deprecated Quarantined nodes are now validated by direct control-plane probes.
     */
    @Deprecated
    public Builder withQuarantineTrafficIdlePeriodMs(long periodMs) {
      this.quarantineTrafficIdlePeriodMs = periodMs;
      return this;
    }

    /**
     * Enables or disables node health tracking.
     *
     * @param disabled true to disable health tracking
     * @return this builder
     */
    public Builder withDisabled(boolean disabled) {
      this.disabled = disabled;
      return this;
    }

    /**
     * Builds a node health configuration.
     *
     * @return node health configuration
     * @throws IllegalArgumentException if probe period/timeout is not positive or concurrency is
     *     outside 1..64
     */
    public NodeHealthConfig build() {
      return new NodeHealthConfig(
          consecutiveFailureThreshold,
          downNodeRecoverySuccessThreshold,
          quarantineSuccessThreshold,
          quarantineFailureThreshold,
          downNodeProbePeriodMs,
          healthProbeConcurrency,
          healthProbeTimeoutMs,
          quarantineTrafficInterval,
          quarantineTrafficIdlePeriodMs,
          disabled);
    }
  }
}
