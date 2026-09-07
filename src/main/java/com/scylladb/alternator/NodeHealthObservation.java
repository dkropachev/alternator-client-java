package com.scylladb.alternator;

/** Result observed for an Alternator node health decision. */
public enum NodeHealthObservation {
  /** A routed DynamoDB request produced a response considered healthy for routing purposes. */
  TRAFFIC_SUCCESS,

  /** A routed DynamoDB request failed before receiving an HTTP response. */
  TRAFFIC_FAILURE,

  /**
   * A direct node-health probe completed successfully; activates quarantine or advances down-node
   * recovery.
   */
  PROBE_SUCCESS,

  /**
   * A direct node-health probe failed; leaves quarantine unchanged or resets down-node recovery.
   */
  PROBE_FAILURE
}
